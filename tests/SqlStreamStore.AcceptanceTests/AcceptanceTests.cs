namespace SqlStreamStore
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Threading.Tasks;
    using MorseCode.ITask;
    using Shouldly;
    using SqlStreamStore.Streams;
    using SqlStreamStore.TestUtils;
    using Xunit;
    using Xunit.Abstractions;

    public abstract partial class AcceptanceTests<TReadAllPage> : IAsyncLifetime where TReadAllPage : IReadAllPage
    {
        private const string DefaultJsonData = @"{ ""data"": ""data"" }";
        private const string DefaultJsonMetadata = @"{ ""meta"": ""data"" }";
        private readonly IDisposable _logCapture;

        protected AcceptanceTests(ITestOutputHelper testOutputHelper)
        {
            TestOutputHelper = testOutputHelper;
            _logCapture = CaptureLogs(testOutputHelper);
        }

        public async Task InitializeAsync()
        {
            Fixture = await CreateFixture();
        }

        private IStreamStore<TReadAllPage> Store => Fixture.Store;

        protected IStreamStoreFixture<TReadAllPage> Fixture { get; private set; }

        protected ITestOutputHelper TestOutputHelper { get; }

        public Task DisposeAsync()
        {
            Fixture.Dispose();
            _logCapture.Dispose();
            return Task.CompletedTask;
        }

        protected abstract Task<IStreamStoreFixture<TReadAllPage>> CreateFixture();

        private static IDisposable CaptureLogs(ITestOutputHelper testOutputHelper) 
            => LoggingHelper.Capture(testOutputHelper);

        [Fact]
        public async Task When_dispose_and_read_then_should_throw()
        {
            Store.Dispose();

            Func<Task> act = () => Store.ReadAllForwards(Position.Start, 10).AsTask();

            await act.ShouldThrowAsync<ObjectDisposedException>();
        }

        [Fact]
        public void Can_dispose_more_than_once()
        {
            Store.Dispose();

            Action act = Store.Dispose;

            act.ShouldNotThrow();
        }

        public static NewStreamMessage[] CreateNewStreamMessages(params int[] messageNumbers)
        {
            return CreateNewStreamMessages(DefaultJsonData, messageNumbers);
        }

        private static NewStreamMessage[] CreateNewStreamMessages(string jsonData, params int[] messageNumbers)
        {
            return messageNumbers
                .Select(number =>
                {
                    var id = Guid.Parse("00000000-0000-0000-0000-" + number.ToString().PadLeft(12, '0'));
                    return new NewStreamMessage(id, "type", jsonData, DefaultJsonMetadata);
                })
                .ToArray();
        }

        public static NewStreamMessage[] CreateNewStreamMessageSequence(int startId, int count)
        {
            var messages = new List<NewStreamMessage>();
            for(int i = 0; i < count; i++)
            {
                var messageNumber = startId + i;
                var messageId = Guid.Parse("00000000-0000-0000-0000-" + messageNumber.ToString().PadLeft(12, '0'));
                var newStreamMessage = new NewStreamMessage(messageId, "type", DefaultJsonData, DefaultJsonMetadata);
                messages.Add(newStreamMessage);
            }
            return messages.ToArray();
        }

        public static StreamMessage ExpectedStreamMessage(
            string streamId,
            int messageNumber,
            int sequenceNumber,
            DateTime created)
        {
            var id = Guid.Parse("00000000-0000-0000-0000-" + messageNumber.ToString().PadLeft(12, '0'));
            return new StreamMessage(streamId, id, sequenceNumber, 0, created, "type", DefaultJsonMetadata, DefaultJsonData);
        }
    }
}
