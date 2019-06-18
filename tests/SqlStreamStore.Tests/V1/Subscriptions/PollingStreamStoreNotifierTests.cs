namespace SqlStreamStore.V1.Subscriptions
{
    using System;
    using System.Threading;
    using System.Threading.Tasks;
    using SqlStreamStore.TestUtils;
    using SqlStreamStore.V1.Infrastructure;
    using Xunit;

    public class PollingStreamStoreNotifierTests
    {
        [Fact]
        public async Task When_exception_occurs_reading_head_position_then_polling_should_continue()
        {
            int readHeadCount = 0;

            Task<long> ReadHeadPosition(CancellationToken _)
            {
                readHeadCount++;
                if(readHeadCount % 2 == 0)
                {
                    throw new Exception("oops");
                }

                return Task.FromResult((long) readHeadCount);
            }

            using(var notifier = new PollingStreamStoreNotifier(ReadHeadPosition, 10))
            {
                int received = 0;
                var tcs = new TaskCompletionSource<Unit>();
                notifier.Subscribe(_ =>
                {
                    received++;
                    if(received > 5)
                    {
                        tcs.SetResult(Unit.Default);
                    }
                });

                await tcs.Task.WithTimeout();
            }
        }
    }
}