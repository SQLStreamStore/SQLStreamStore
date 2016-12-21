namespace SqlStreamStore.Postgres
{
    using System;
    using System.Threading;
    using System.Threading.Tasks;
    using SqlStreamStore.Infrastructure;
    using SqlStreamStore.Streams;
    using SqlStreamStore.Subscriptions;

    public class PostgresStreamStore : StreamStoreBase
    {
        private PostgresStreamStoreSettings _settings;

        public PostgresStreamStore(PostgresStreamStoreSettings settings)
            : base(settings.MetadataMaxAgeCacheExpire, settings.MetadataMaxAgeCacheMaxSize, settings.GetUtcNow, settings.LogName)
        {
            this._settings = settings;
        }

        protected override Task<ReadAllPage> ReadAllForwardsInternal(long fromPositionExlusive, int maxCount, bool prefetch, ReadNextAllPage readNext, CancellationToken cancellationToken)
        {
            throw new NotImplementedException();
        }

        protected override Task<ReadAllPage> ReadAllBackwardsInternal(long fromPositionExclusive, int maxCount, bool prefetch, ReadNextAllPage readNext, CancellationToken cancellationToken)
        {
            throw new NotImplementedException();
        }

        protected override Task<ReadStreamPage> ReadStreamForwardsInternal(string streamId, int start, int count, bool prefetch, ReadNextStreamPage readNext, CancellationToken cancellationToken)
        {
            throw new NotImplementedException();
        }

        protected override Task<ReadStreamPage> ReadStreamBackwardsInternal(string streamId, int fromVersionInclusive, int count, bool prefetch, ReadNextStreamPage readNext, CancellationToken cancellationToken)
        {
            throw new NotImplementedException();
        }

        protected override Task<long> ReadHeadPositionInternal(CancellationToken cancellationToken)
        {
            throw new NotImplementedException();
        }

        protected override IStreamSubscription SubscribeToStreamInternal(
            string streamId,
            int? startVersion,
            StreamMessageReceived streamMessageReceived,
            SubscriptionDropped subscriptionDropped,
            HasCaughtUp hasCaughtUp,
            bool prefetchJsonData,
            string name)
        {
            throw new NotImplementedException();
        }

        protected override IAllStreamSubscription SubscribeToAllInternal(
            long? fromPosition,
            AllStreamMessageReceived streamMessageReceived,
            AllSubscriptionDropped subscriptionDropped,
            HasCaughtUp hasCaughtUp,
            bool prefetchJsonData,
            string name)
        {
            throw new NotImplementedException();
        }

        protected override Task<StreamMetadataResult> GetStreamMetadataInternal(string streamId, CancellationToken cancellationToken)
        {
            throw new NotImplementedException();
        }

        public override Task<int> GetmessageCount(string streamId, CancellationToken cancellationToken = new CancellationToken())
        {
            throw new NotImplementedException();
        }

        public Task<int> GetmessageCount(string streamId, DateTime TODO_WHAT_IS_THIS_FOR, CancellationToken cancellationToken = new CancellationToken())
        {
            throw new NotImplementedException();
        }

        protected override Task<AppendResult> AppendToStreamInternal(
            string streamId,
            int expectedVersion,
            NewStreamMessage[] messages,
            CancellationToken cancellationToken)
        {
            throw new NotImplementedException();
        }

        protected override Task DeleteStreamInternal(string streamId, int expectedVersion, CancellationToken cancellationToken)
        {
            throw new NotImplementedException();
        }

        protected override Task DeleteEventInternal(string streamId, Guid eventId, CancellationToken cancellationToken)
        {
            throw new NotImplementedException();
        }

        protected override Task SetStreamMetadataInternal(
            string streamId,
            int expectedStreamMetadataVersion,
            int? maxAge,
            int? maxCount,
            string metadataJson,
            CancellationToken cancellationToken)
        {
            throw new NotImplementedException();
        }

        public Task DropAll(bool ignoreErrors)
        {
            throw new NotImplementedException();
        }
    }
}
