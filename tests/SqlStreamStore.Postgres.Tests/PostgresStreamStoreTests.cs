namespace SqlStreamStore
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Threading;
    using System.Threading.Tasks;
    using Shouldly;
    using SqlStreamStore.Infrastructure;
    using SqlStreamStore.Streams;
    using SqlStreamStore.TestUtils;
    using SqlStreamStore.TestUtils.Postgres;
    using Xunit;
    using Xunit.Abstractions;

    public class PostgresStreamStoreTests : IDisposable
    {
        private readonly PostgresContainer _container;
        private const string Schema = "stresstest";
        private readonly ITestOutputHelper _testOutputHelper;
        private readonly IDisposable _logCapture;

        public PostgresStreamStoreTests(ITestOutputHelper testOutputHelper)
        {
            _container = new PostgresContainer(Schema, $"test_{Guid.NewGuid():n}", 0.02f);
            _testOutputHelper = testOutputHelper;
            _logCapture = LoggingHelper.Capture(testOutputHelper);
        }

        [Fact]
        public async Task Can_Handle_High_Load_Gaps()
        {
            await _container.Start();
            await _container.CreateDatabase();

            var readerSettings = GetSettings(_container.GenerateConnectionString("reader"));
            var writerSettings = GetSettings(_container.GenerateConnectionString("writer"));

            using var readerStore = new PostgresStreamStore(readerSettings);
            using var writerStore = new PostgresStreamStore(writerSettings);

            await writerStore.CreateSchemaIfNotExists();

            var receiveMessages = new TaskCompletionSource<StreamMessage>();
            List<StreamMessage> receivedMessages = new List<StreamMessage>();

            using var all = readerStore.SubscribeToAll(
                Position.None,
                (_, message, __) =>
                {
                    _testOutputHelper.WriteLine($"Received message {message.StreamId} " +
                                               $"{message.StreamVersion} {message.Position}");
                    if (message.Position >= 200)
                        receiveMessages.SetResult(message);
                    else
                        receivedMessages.Add(message);

                    return Task.CompletedTask;
                });

            all.MaxCountPerRead = 500;

            await AppendMessages(writerStore, 1000);
            await receiveMessages.Task.WithTimeout(1000000);

            receivedMessages.Count.ShouldBe(100);
        }

        private static PostgresStreamStoreSettings GetSettings(string connectionString)
        {
            return new PostgresStreamStoreSettings(connectionString)
            {
                Schema = Schema,
                GetUtcNow = SystemClock.GetUtcNow,
                DisableDeletionTracking = false,
                ScavengeAsynchronously = false
            };
        }

        private static async Task AppendMessages(IStreamStore streamStore, int numberOfEvents)
        {
            await Task.WhenAll(Enumerable.Range(0, numberOfEvents).Select(_ =>
            {
                var newMessage = new NewStreamMessage(Guid.NewGuid(), "MyEvent", "{}");
                return streamStore.AppendToStream(Guid.NewGuid().ToString(), ExpectedVersion.Any, newMessage);
            }));
        }

        public void Dispose()
        {
            _container.Dispose();
            _logCapture.Dispose();
        }
    }
}