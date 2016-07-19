namespace Cedar.EventStore.Scavenging
{
    using System;
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
                (reason, exception) =>
                {
                    Console.WriteLine(reason);
                });
        }

        public long? GetCheckpoint()
        {
            using(_lock.UseReadLock())
            {
                return _currentCheckpoint;
            }
        }

        public ScavengerStreamEvent[] GetStream(string streamId)
        {
            using(_lock.UseReadLock())
            {
                return _streamEventCollection.GetStream(streamId);
            }
        }

        public ScavengerStreamEvent GetStreamEvent(string streamId, Guid eventId)
        {
            using (_lock.UseReadLock())
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
                metadataMessage.StreamId,
                metadataMessage.MaxAge,
                metadataMessage.MaxCount);

            _streamEventCollection.AddOrUpdate(streamMetadata);
        }


        private void HandleEventDeleted(StreamEvent streamEvent)
        {
            var eventDeleted = streamEvent.JsonDataAs<Deleted.EventDeleted>();
            _streamEventCollection.RemoveEvent(eventDeleted.StreamId, eventDeleted.EventId);
        }

        private void HandleStreamDeleted(StreamEvent streamEvent)
        {
            var eventDeleted = streamEvent.JsonDataAs<Deleted.StreamDeleted>();
            _streamEventCollection.RemoveStream(eventDeleted.StreamId);
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