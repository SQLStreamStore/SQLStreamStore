namespace SqlStreamStore.Infrastructure
{
    using System;
    using System.Threading;
    using System.Threading.Tasks;
    using EnsureThat;
    using SqlStreamStore.Streams;
    using SqlStreamStore;

    public abstract class StreamStoreBase : ReadonlyStreamStoreBase, IStreamStore
    {
        private readonly TaskQueue _taskQueue = new TaskQueue();

        protected StreamStoreBase(
            TimeSpan metadataMaxAgeCacheExpiry,
            int metadataMaxAgeCacheMaxSize,
            GetUtcNow getUtcNow,
            string logName)
            : base(metadataMaxAgeCacheExpiry, metadataMaxAgeCacheMaxSize, getUtcNow, logName)
        {}

        public Task AppendToStream(
            string streamId,
            int expectedVersion,
            NewStreamMessage[] messages,
            CancellationToken cancellationToken = default(CancellationToken))
        {
            Ensure.That(streamId, nameof(streamId)).IsNotNullOrWhiteSpace().DoesNotStartWith("$");

            return AppendToStreamInternal(streamId, expectedVersion, messages, cancellationToken);
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

        protected override void PurgeExpiredEvent(StreamMessage streamMessage)
        {
            _taskQueue.Enqueue(ct => DeleteEventInternal(streamMessage.StreamId, streamMessage.EventId, ct));
        }

        protected abstract Task AppendToStreamInternal(
            string streamId,
            int expectedVersion,
            NewStreamMessage[] messages,
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