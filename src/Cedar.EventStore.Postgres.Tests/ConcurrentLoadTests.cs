namespace Cedar.EventStore.Postgres.Tests
{
    using Exceptions;
    using System;
    using System.Threading.Tasks;
    using Xunit;
    using Xunit.Abstractions;

    public class ConcurrentLoadTests
    {
        public ConcurrentLoadTests(ITestOutputHelper testOutputHelper)
        { }

        [Fact]
        public async Task conccurrent_appends_might_throw_WrongExpectedVersionException_and_thats_ok()
        {
            var eventStore = new PostgresEventStore(@"Server=127.0.0.1;Port=5432;Database=cedar_tests;User Id=postgres;Password=postgres;");

            await eventStore.DropAll(ignoreErrors: true);
            await eventStore.InitializeStore();

            using (eventStore)
            {
                for (var i = 0; i < 3; i++)
                {
                    Parallel.For(0, 4, async (iteration) =>
                    {
                        var streamId = string.Concat("stream-", iteration);
                        await eventStore
                            .AppendToStream(streamId, ExpectedVersion.Any, new NewStreamEvent(Guid.NewGuid(), "type", "\"data\"", "\"metadata\""))
                            .MightThrow<WrongExpectedVersionException>("Append failed due to WrongExpectedVersion. Stream: {0}, Expected version: -2".FormatWith(streamId));
                    });

                }
            }
        }
    }
}
