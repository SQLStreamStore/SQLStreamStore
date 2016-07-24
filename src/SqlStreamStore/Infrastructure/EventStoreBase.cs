namespace StreamStore.Infrastructure
{
    using System;
    using System.Threading;
    using System.Threading.Tasks;
    using EnsureThat;
    using StreamStore;
    using StreamStore.Streams;

    public abstract class EventStoreBase : ReadOnlyEventStoreBase, IEventStore
    {
        private readonly TaskQueue _taskQueue = new TaskQueue();

        protected EventStoreBase(
            TimeSpan metadataMaxAgeCacheExpiry,
            int metadataMaxAgeCacheMaxSize,
            GetUtcNow getUtcNow,
            string logName)
            : base(metadataMaxAgeCacheExpiry, metadataMaxAgeCacheMaxSize, getUtcNow, logName)
        {}

        public Task AppendToStream(
            string streamId,
            int expectedVersion,
            NewStreamEvent[] events,
            CancellationToken cancellationToken = default(CancellationToken))
        {
            Ensure.That(streamId, nameof(streamId)).IsNotNullOrWhiteSpace().DoesNotStartWith("$");

            return AppendToStreamInternal(streamId, expectedVersion, events, cancellationToken);
        }

        public Task DeleteStream(
            string streamId,
            int expectedVersion = ExpectedVersion.Any,
            CancellationToken cancellationToken = default(CancellationToken))
        {
            Ensure.That(streamId, nameof(streamId)).IsNotNullOrWhiteSpace().DoesNotStartWith("$");

            return DeleteStreamInternal(streamId, expectedVersion, cancellationToken);
        }

        public Task DeleteEvent(
            string streamId,
            Guid eventId,
            CancellationToken cancellationToken = default(CancellationToken))
        {
            Ensure.That(streamId, nameof(streamId)).IsNotNullOrWhiteSpace().DoesNotStartWith("$");

            return DeleteEventInternal(streamId, eventId, cancellationToken);
        }

        public Task SetStreamMetadata(
            string streamId,
            int expectedStreamMetadataVersion,
            int? maxAge = null,
            int? maxCount = null,
            string metadataJson = null,
            CancellationToken cancellationToken = default(CancellationToken))
        {
            Ensure.That(streamId, nameof(streamId)).IsNotNullOrWhiteSpace().DoesNotStartWith("$");
            Ensure.That(expectedStreamMetadataVersion, nameof(expectedStreamMetadataVersion)).IsGte(-2);

            return SetStreamMetadataInternal(
                streamId,
                expectedStreamMetadataVersion,
                maxAge,
                maxCount,
                metadataJson,
                cancellationToken);
        }

        public abstract Task<int> GetStreamEventCount(
            string streamId,
            CancellationToken cancellationToken = default(CancellationToken));

        protected override void PurgeExpiredEvent(StreamEvent streamEvent)
        {
            _taskQueue.Enqueue(ct => DeleteEventInternal(streamEvent.StreamId, streamEvent.EventId, ct));
        }

        protected abstract Task AppendToStreamInternal(
            string streamId,
            int expectedVersion,
            NewStreamEvent[] events,
            CancellationToken cancellationToken);

        protected abstract Task DeleteStreamInternal(
            string streamId,
            int expectedVersion,
            CancellationToken cancellationToken);

        protected abstract Task DeleteEventInternal(
            string streamId,
            Guid eventId,
            CancellationToken cancellationToken);

        protected abstract Task SetStreamMetadataInternal(
           string streamId,
           int expectedStreamMetadataVersion,
           int? maxAge,
           int? maxCount,
           string metadataJson,
           CancellationToken cancellationToken);

        protected override void Dispose(bool disposing)
        {
            if(disposing)
            {
                _taskQueue.Dispose();
            }
            base.Dispose(disposing);
        }
    }
}