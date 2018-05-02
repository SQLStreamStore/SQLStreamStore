namespace SqlStreamStore
{
    using System;
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.Linq;
    using System.Threading;
    using System.Threading.Tasks;
    using Shouldly;
    using SqlStreamStore.Infrastructure;
    using SqlStreamStore.Streams;
    using SqlStreamStore.Subscriptions;
    using Xunit;

    public class GapHandling
    {
        [Fact, Trait("Category", "ReadAll")]
        public async Task When_read_all_forwards_handles_internal_transient_gaps_gracefully()
        {
            var messagePositionsForBatch = new[]
            {
                new long[] {1, 3},              // on the initial read, message 2 has not ben committed
                new long[] {1, 2, 3, 4, 6},     // by the second read, message 3 has appeared, but so have messages 4 & 6, with 5 now missing
                new long[] {1, 2, 3, 4, 5, 6},  // by the third read, the page is internally consitent
            };

            using (var store = new GappyScenarioStore(messagePositionsForBatch))
            {
                var results = await store.ReadAllForwards(123, 1000, false);

                results.Messages.Select(x => x.Position).ShouldBe(new long[] { 1, 2, 3, 4, 5, 6 });
                store.InvocationCount.ShouldBe(3);
            }
        }


        [Fact, Trait("Category", "ReadAll")]
        public async Task When_read_all_forwards_handles_internal_permenant_gaps_gracefully()
        {
            var messagePositionsForBatch = new[]
            {
                new long[] {1, 4},           // on the initial read, message 2 has not ben committed
                new long[] {1, 4, 5, 6},     // by the second read, 2 still does not exist, so we assume it won't (w.g. a tx has ben rolled back)
            };


            using (var store = new GappyScenarioStore(messagePositionsForBatch))
            {
                var results = await store.ReadAllForwards(1, 1000, false);

                results.Messages.Select(x => x.Position).ShouldBe(new long[] { 1, 4, 5, 6 });
                store.InvocationCount.ShouldBe(3);
            }
        }

        [Fact, Trait("Category", "ReadAll")]
        public async Task When_read_all_forwards_handles_gaps_since_last_page_gracefully()
        {
            var messagePositionsForBatch = new[]
            {
                new long[] {4, 5, 6},     // on the initial read, message 2 has not ben committed
                new long[] {4, 5, 6},     // by the second read, 2 still does not exist, so we assume it won't (w.g. a tx has ben rolled back)
            };


            using (var store = new GappyScenarioStore(messagePositionsForBatch))
            {
                var results = await store.ReadAllForwards(1, 1000, false);

                results.Messages.Select(x => x.Position).ShouldBe(new long[] { 4, 5, 6 });
                store.InvocationCount.ShouldBe(2);
            }
        }

        public class GappyScenarioStore : ReadonlyStreamStoreBase
        {
            private readonly long[][] positions;

            public int InvocationCount { get; private set; }

            public GappyScenarioStore(long[][] positions) : base(TimeSpan.Zero, 0, () => DateTime.UtcNow, "")
            {
                this.positions = positions;
            }

            protected override Task<ReadAllPage> ReadAllForwardsInternal(long fromPositionExlusive, int maxCount, bool prefetch, ReadNextAllPage readNext, CancellationToken cancellationToken)
            {
                if (InvocationCount == positions.Length)
                {
                    throw new Exception("More invocations than have been configured.");
                }

                var positionsForInvocation = positions[InvocationCount];

                var messages = positionsForInvocation.Select(x =>
                    new StreamMessage("some-stream", Guid.Empty, 0, x, DateTime.UtcNow, "", "", ct => Task.FromResult("")));

                var result = new ReadAllPage(fromPositionExlusive, long.MaxValue, true, ReadDirection.Forward, null, messages.ToArray());

                InvocationCount++;

                return Task.FromResult(result);
            }

            protected override Task<StreamMetadataResult> GetStreamMetadataInternal(string streamId, CancellationToken cancellationToken)
            {
                var resulit = new StreamMetadataResult("some-stream", 1);

                return Task.FromResult(resulit);
            }

            protected override Task<ReadAllPage> ReadAllBackwardsInternal(long fromPositionExclusive, int maxCount, bool prefetch, ReadNextAllPage readNext, CancellationToken cancellationToken)
            { throw new NotImplementedException(); }

            protected override Task<ReadStreamPage> ReadStreamForwardsInternal(string streamId, int start, int count, bool prefetch, ReadNextStreamPage readNext, CancellationToken cancellationToken)
            { throw new NotImplementedException(); }

            protected override Task<ReadStreamPage> ReadStreamBackwardsInternal(string streamId, int fromVersionInclusive, int count, bool prefetch, ReadNextStreamPage readNext, CancellationToken cancellationToken)
            { throw new NotImplementedException(); }

            protected override Task<long> ReadHeadPositionInternal(CancellationToken cancellationToken)
            { throw new NotImplementedException(); }

            protected override IStreamSubscription SubscribeToStreamInternal(string streamId, int? startVersion, StreamMessageReceived streamMessageReceived, SubscriptionDropped subscriptionDropped, HasCaughtUp hasCaughtUp, bool prefetchJsonData, string name)
            { throw new NotImplementedException(); }

            protected override IAllStreamSubscription SubscribeToAllInternal(long? fromPosition, AllStreamMessageReceived streamMessageReceived, AllSubscriptionDropped subscriptionDropped, HasCaughtUp hasCaughtUp, bool prefetchJsonData, string name)
            { throw new NotImplementedException(); }

            protected override void PurgeExpiredMessage(StreamMessage streamMessage)
            { throw new NotImplementedException(); }
        }
    }
}