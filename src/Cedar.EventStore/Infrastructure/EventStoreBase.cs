namespace Cedar.EventStore.Infrastructure
{
    using System;
    using System.Threading;
    using System.Threading.Tasks;
    using Cedar.EventStore.Streams;
    using EnsureThat;

    public abstract class EventStoreBase : ReadOnlyEventStoreBase, IEventStore
    {
        protected EventStoreBase(string logName = null)
            : base(logName)
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

        public Task<StreamMetadataResult> GetStreamMetadata(
            string streamId,
            CancellationToken cancellationToken = default(CancellationToken))
        {
            Ensure.That(streamId, nameof(streamId)).IsNotNullOrWhiteSpace().DoesNotStartWith("$");

            return GetStreamMetadataInternal(streamId, cancellationToken);
        }

        public Task SetStreamMetadata(
            string streamId,
            int expectedStreamMetadataVersion,
            int? maxAge,
            int? maxCount,
            string metadataJson,
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

        protected abstract Task<StreamMetadataResult> GetStreamMetadataInternal(
            string streamId,
            CancellationToken cancellationToken);

        protected abstract Task SetStreamMetadataInternal(
           string streamId,
           int expectedStreamMetadataVersion,
           int? maxAge,
           int? maxCount,
           string metadataJson,
           CancellationToken cancellationToken);

        protected async Task CheckStreamMaxCount(string streamId, int? maxCount, CancellationToken cancellationToken)
        {
            if (maxCount.HasValue)
            {
                var count = await GetStreamEventCount(streamId, cancellationToken);
                if (count > maxCount.Value)
                {
                    int toPurge = count - maxCount.Value;

                    var streamEventsPage = await ReadStreamForwardsInternal(streamId, StreamVersion.Start,
                        toPurge, cancellationToken);

                    if (streamEventsPage.Status == PageReadStatus.Success)
                    {
                        foreach (var streamEvent in streamEventsPage.Events)
                        {
                            await DeleteEventInternal(streamId, streamEvent.EventId, cancellationToken);
                        }
                    }
                }
            }
        }
    }
}