namespace Cedar.EventStore.Scavenging
{
    using System;
    using System.Collections.Generic;
    using System.Threading;
    using System.Threading.Tasks;
    using Cedar.EventStore.Infrastructure;
    using Cedar.EventStore.Streams;
    using Timer = System.Timers.Timer;

    public class InMemoryScavenger : IDisposable
    {
        private readonly IEventStore _eventStore;
        private readonly GetUtcNow _getUtcNow;
        private readonly TaskQueue _taskQueue = new TaskQueue();
        private long? _currentCheckpoint;
        private IAllStreamSubscription _allStreamSubscription;
        private readonly Dictionary<string, Dictionary<Guid, ScavengerStreamEvent>> _streamEventsByStream 
            = new Dictionary<string, Dictionary<Guid, ScavengerStreamEvent>>();
        private readonly Dictionary<string, ScavengerStreamMetadata> _streamMetadata 
            = new Dictionary<string, ScavengerStreamMetadata>();
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
                foreach(var stream in _streamEventsByStream)
                {
                    foreach(var scavengerStreamEvent in stream.Value.Values)
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
                if (!_streamEventsByStream.ContainsKey(streamId))
                {
                    return null;
                }
                if (!_streamEventsByStream[streamId].ContainsKey(eventId))
                {
                    return null;
                }
                return _streamEventsByStream[streamId][eventId];
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
            var streamId = streamEvent.StreamId;
            var metadataMessage = streamEvent.JsonDataAs<MetadataMessage>();

            var scavengerStreamMetadata = new ScavengerStreamMetadata(
                streamEvent.StreamId,
                metadataMessage.MaxAge,
                metadataMessage.MaxCount);

            if (!_streamMetadata.ContainsKey(streamEvent.StreamId))
            {
                _streamMetadata.Add(streamId, scavengerStreamMetadata);
            }
            else
            {
                _streamMetadata[streamId] = scavengerStreamMetadata;
            }

            ResetStreamEventExpiry(metadataMessage, scavengerStreamMetadata);
        }

        private void ResetStreamEventExpiry(MetadataMessage metadataMessage, ScavengerStreamMetadata scavengerStreamMetadata)
        {
            Dictionary<Guid, ScavengerStreamEvent> scavengerStreamEvents;
            if(_streamEventsByStream.TryGetValue(metadataMessage.StreamId, out scavengerStreamEvents))
            {
                foreach(var value in scavengerStreamEvents.Values)
                {
                    value.SetExpires(scavengerStreamMetadata.MaxAge);
                }
            }
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

            if (!_streamEventsByStream.ContainsKey(streamId))
            {
                _streamEventsByStream.Add(streamId, new Dictionary<Guid, ScavengerStreamEvent>());
            }
            if (!_streamEventsByStream[streamId].ContainsKey(eventId))
            {
                var scavengerStreamEvent = new ScavengerStreamEvent(
                    streamId, eventId, streamEvent.Created);

                // if the stream has metadata associated with it, adjust the expiry accordingly.
                ScavengerStreamMetadata metadata;
                if(_streamMetadata.TryGetValue($"$${streamId}", out metadata))
                {
                    if(metadata.MaxAge.HasValue)
                    {
                        scavengerStreamEvent.SetExpires(metadata.MaxAge.Value);
                    }
                }

                _streamEventsByStream[streamId].Add(streamEvent.EventId, scavengerStreamEvent);
            }
        }

        public void Dispose()
        {
            _maxAgePurgeTimer.Dispose();
            _allStreamSubscription.Dispose();
            _taskQueue.Dispose();
        }
    }
}