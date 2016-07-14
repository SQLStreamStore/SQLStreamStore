namespace Cedar.EventStore.Scavenging
{
    using System;
    using System.Collections.Generic;
    using System.Threading;
    using System.Threading.Tasks;
    using Cedar.EventStore.Infrastructure;
    using Cedar.EventStore.Streams;
    using Timer = System.Timers.Timer;

    public class InMemoryScavenger
    {
        private readonly IEventStore _eventStore;
        private readonly GetUtcNow _getUtcNow;
        private readonly ConcurrentExclusiveSchedulerPair _scheduler;
        private long? _currentCheckpoint;
        private IAllStreamSubscription _allStreamSubscription;
        private readonly Dictionary<string, Dictionary<Guid, ScavengerStreamEvent>> _streamEventsByStream 
            = new Dictionary<string, Dictionary<Guid, ScavengerStreamEvent>>();
        private readonly Dictionary<string, ScavengerStreamMetadata> _streamMetadata 
            = new Dictionary<string, ScavengerStreamMetadata>();
        private readonly Timer _maxAgePurgeTimer;
        private TaskFactory _taskFactory;

        public InMemoryScavenger(IEventStore eventStore, GetUtcNow getUtcNow = null, int purgeInterval = 1000)
        {
            _eventStore = eventStore;
            _getUtcNow = getUtcNow;
            _scheduler = new ConcurrentExclusiveSchedulerPair();
            _taskFactory = new TaskFactory(_scheduler.ExclusiveScheduler);
            _maxAgePurgeTimer = new Timer(purgeInterval)
            {
                AutoReset = true,
                Enabled = true
            };
            _maxAgePurgeTimer.Elapsed += (_, __) => 
            {
                PurgeExpiredEvents();
            };
        }

        public Task PurgeExpiredEvents()
        {
            return StartNewTask(() =>
            {
                var utcNow = _getUtcNow();
                foreach(var stream in _streamEventsByStream)
                {
                    foreach(var scavengerStreamEvent in stream.Value.Values)
                    {
                        if(scavengerStreamEvent.Expires < utcNow)
                        {
                            StartNewTask(async () => 
                                await _eventStore.DeleteEvent(scavengerStreamEvent.StreamId, scavengerStreamEvent.EventId));
                        }
                    }
                }
            });
        }

        public event EventHandler<StreamEvent> StreamEventProcessed;
        
        public Task Complete()
        {
            _maxAgePurgeTimer?.Dispose();
            _allStreamSubscription?.Dispose();
            _scheduler.Complete();
            return _scheduler.Completion;
        }

        public Task Initialize()
        {
            return StartNewTask(async () =>
            {
                _allStreamSubscription = await _eventStore.SubscribeToAll(
                   _currentCheckpoint,
                   ProcessStreamEvent,
                   (reason, exception) => { });
            });
        }

        public Task<long?> GetCheckpoint()
        {
            return StartNewTask(() => _currentCheckpoint);
        }

        public Task<ScavengerStreamEvent> GetStreamEvent(string streamId, Guid eventId)
        {
            return StartNewTask(() =>
            {
                if(!_streamEventsByStream.ContainsKey(streamId))
                {
                    return null;
                }
                if(!_streamEventsByStream[streamId].ContainsKey(eventId))
                {
                    return null;
                }
                return _streamEventsByStream[streamId][eventId];
            });
        }

        private Task<T> StartNewTask<T>(Func<T> func)
        {
            return Task.Factory.StartNew(func, CancellationToken.None, TaskCreationOptions.PreferFairness, _scheduler.ExclusiveScheduler);
        }

        private Task StartNewTask(Action action)
        {
            return _taskFactory.StartNew(
                action,
                CancellationToken.None);
        }

        private void RaiseStreamEventProcessed(StreamEvent streamEvent)
        {
            Volatile.Read(ref StreamEventProcessed)?.Invoke(this, streamEvent);
        }

        private Task ProcessStreamEvent(StreamEvent streamEvent)
        {
            return StartNewTask(() =>
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
    }
}