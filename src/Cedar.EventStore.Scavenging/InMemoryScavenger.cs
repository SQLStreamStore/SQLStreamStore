namespace Cedar.EventStore.Scavenging
{
    using System;
    using System.Collections.Generic;
    using System.Collections.ObjectModel;
    using System.Linq;
    using System.Threading;
    using System.Threading.Tasks;
    using Cedar.EventStore.Infrastructure;
    using Cedar.EventStore.Streams;
    using Timer = System.Timers.Timer;

    internal class StreamEventCollection
    {
        private readonly Dictionary<string, Dictionary<Guid, ScavengerStreamEvent>> _streamEventsByEventId
           = new Dictionary<string, Dictionary<Guid, ScavengerStreamEvent>>();
        private readonly Dictionary<string, LinkedList<ScavengerStreamEvent>> _streamEvents
            = new Dictionary<string, LinkedList<ScavengerStreamEvent>>();
        private readonly Dictionary<string, ScavengerStreamMetadata> _streamMetadata
            = new Dictionary<string, ScavengerStreamMetadata>();

        internal void Add(ScavengerStreamEvent streamEvent)
        {
            var streamId = streamEvent.StreamId;
            var eventId = streamEvent.EventId;

            if (!_streamEventsByEventId.ContainsKey(streamId))
            {
                _streamEventsByEventId.Add(streamId, new Dictionary<Guid, ScavengerStreamEvent>());
            }

            if (!_streamEvents.ContainsKey(streamId))
            {
                _streamEvents.Add(streamId, new LinkedList<ScavengerStreamEvent>());
            }

            // idempotent handling. If we've already seen this event, then skip processing
            if (!_streamEventsByEventId[streamId].ContainsKey(eventId)) 
            {
                var scavengerStreamEvent = new ScavengerStreamEvent(
                    streamId, eventId, streamEvent.Created);

                // if the stream has metadata associated with it, adjust the expiry accordingly.
                ScavengerStreamMetadata metadata;
                if (_streamMetadata.TryGetValue($"$${streamId}", out metadata))
                {
                    if (metadata.MaxAge.HasValue)
                    {
                        scavengerStreamEvent.SetExpires(metadata.MaxAge.Value);
                    }
                }

                _streamEventsByEventId[streamId].Add(streamEvent.EventId, scavengerStreamEvent);
                _streamEvents[streamId].AddLast(scavengerStreamEvent);
            }
        }

        internal IReadOnlyCollection<string> GetStreams()
        {
            return new ReadOnlyCollection<string>(_streamEventsByEventId.Keys.ToList());
        }

        internal IReadOnlyDictionary<Guid, ScavengerStreamEvent> GetEvents(string streamId)
        {
            return new ReadOnlyDictionary<Guid, ScavengerStreamEvent>(_streamEventsByEventId[streamId]);
        }

        internal void AddOrUpdate(ScavengerStreamMetadata metadata)
        {
            if (!_streamMetadata.ContainsKey(metadata.StreamId))
            {
                _streamMetadata.Add(metadata.StreamId, metadata);
            }
            else
            {
                _streamMetadata[metadata.StreamId] = metadata;
            }

            // Reset the expiry stamps for events affected by the metadata change.
            Dictionary<Guid, ScavengerStreamEvent> scavengerStreamEvents;
            if (_streamEventsByEventId.TryGetValue(metadata.StreamId, out scavengerStreamEvents))
            {
                foreach (var value in scavengerStreamEvents.Values)
                {
                    value.SetExpires(metadata.MaxAge);
                }
            }
        }

        internal ScavengerStreamEvent GetStreamEvent(string streamId, Guid eventId)
        {
            if (!_streamEventsByEventId.ContainsKey(streamId))
            {
                return null;
            }
            if (!_streamEventsByEventId[streamId].ContainsKey(eventId))
            {
                return null;
            }
            return _streamEventsByEventId[streamId][eventId];
        }
    }

    public class InMemoryScavenger : IDisposable
    {
        private readonly IEventStore _eventStore;
        private readonly GetUtcNow _getUtcNow;
        private readonly TaskQueue _taskQueue = new TaskQueue();
        private long? _currentCheckpoint;
        private IAllStreamSubscription _allStreamSubscription;
        private readonly StreamEventCollection _streamEventCollection = new StreamEventCollection();
        private readonly Timer _maxAgePurgeTimer;
        private readonly ReaderWriterLockSlim _lock = new ReaderWriterLockSlim();

        public InMemoryScavenger(IEventStore eventStore, GetUtcNow getUtcNow = null, int scavangeInterval = 60000)
        {
            _eventStore = eventStore;
            _getUtcNow = getUtcNow;
            _maxAgePurgeTimer = new Timer(scavangeInterval)
            {
                AutoReset = false,
                Enabled = true
            };
            _maxAgePurgeTimer.Elapsed += (_, __) => 
            {
                ScavangeNow();
            };
        }

        public Task ScavangeNow()
        {
            return _taskQueue.Enqueue(async ct =>
            {
                var utcNow = _getUtcNow();
                foreach(var stream in _streamEventCollection.GetStreams())
                {
                    foreach(var scavengerStreamEvent in _streamEventCollection.GetEvents(stream).Values)
                    {
                        ct.ThrowIfCancellationRequested();

                        if(scavengerStreamEvent.Expires < utcNow)
                        {
                            // todo, retries and exception logging.
                            await _eventStore
                                .DeleteEvent(scavengerStreamEvent.StreamId, scavengerStreamEvent.EventId, ct);
                        }
                    }
                }
                _maxAgePurgeTimer.Start();
            });
        }

        public event EventHandler<StreamEvent> StreamEventProcessed;
        
        public async Task Initialize()
        {
            _allStreamSubscription = await _eventStore.SubscribeToAll(
                   _currentCheckpoint,
                   StreamEventReceived,
                   (reason, exception) => { });
        }

        public long? GetCheckpoint()
        {
            using(_lock.UseReadLock())
            {
                return _currentCheckpoint;
            }
        }

        public ScavengerStreamEvent GetStreamEvent(string streamId, Guid eventId)
        {
            using(_lock.UseReadLock())
            {
                return _streamEventCollection.GetStreamEvent(streamId, eventId);
            }
        }

        private void RaiseStreamEventProcessed(StreamEvent streamEvent)
        {
            Volatile.Read(ref StreamEventProcessed)?.Invoke(this, streamEvent);
        }

        private Task StreamEventReceived(StreamEvent streamEvent)
        {
            return _taskQueue.Enqueue(() =>
            {
                using(_lock.UseWriteLock())
                {
                    if(streamEvent.StreamId.StartsWith("$$"))
                    {
                        HandleMetadataEvent(streamEvent);
                    }
                    else if(streamEvent.StreamId == Deleted.DeletedStreamId
                            && streamEvent.Type == Deleted.EventDeletedEventType)
                    {
                        HandleEventDeleted(streamEvent);
                    }
                    else if(streamEvent.StreamId == Deleted.DeletedStreamId
                            && streamEvent.Type == Deleted.StreamDeletedEventType)
                    {
                        HandleStreamDeleted(streamEvent);
                    }
                    else
                    {
                        HandleStreamEvent(streamEvent);
                    }

                    _currentCheckpoint = streamEvent.Checkpoint;
                }
                RaiseStreamEventProcessed(streamEvent);
            });
        }

        private void HandleMetadataEvent(StreamEvent streamEvent)
        {
            var metadataMessage = streamEvent.JsonDataAs<MetadataMessage>();

            var streamMetadata = new ScavengerStreamMetadata(
                streamEvent.StreamId,
                metadataMessage.MaxAge,
                metadataMessage.MaxCount);

            _streamEventCollection.AddOrUpdate(streamMetadata);
        }


        private void HandleEventDeleted(StreamEvent streamEvent)
        {

        }

        private void HandleStreamDeleted(StreamEvent streamEvent)
        {

        }

        private void HandleStreamEvent(StreamEvent streamEvent)
        {
            var streamId = streamEvent.StreamId;
            var eventId = streamEvent.EventId;

            var scavengerStreamEvent = new ScavengerStreamEvent(
                    streamId, eventId, streamEvent.Created);
            _streamEventCollection.Add(scavengerStreamEvent);
        }

        private void CheckStreamMaxCount(string streamId)
        {
            _taskQueue.Enqueue(() =>
            {
                using(_lock.UseReadLock())
                {
                }
            });
        }

        public void Dispose()
        {
            _maxAgePurgeTimer.Dispose();
            _allStreamSubscription.Dispose();
            _taskQueue.Dispose();
        }
    }
}