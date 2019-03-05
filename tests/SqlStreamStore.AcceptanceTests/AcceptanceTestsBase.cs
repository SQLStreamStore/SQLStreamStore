﻿namespace SqlStreamStore
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Threading.Tasks;
    using Shouldly;
    using SqlStreamStore.Streams;
    using Xunit;
    using Xunit.Abstractions;

    public abstract class AcceptanceTestsBase : IAsyncLifetime
    {
        private const string DefaultJsonData = @"{ ""data"": ""data"" }";
        private const string DefaultJsonMetadata = @"{ ""meta"": ""data"" }";

        private readonly IDisposable _logCapture;
        private IStreamStoreFixture _fixture;

        protected ITestOutputHelper TestOutputHelper { get; }

        protected AcceptanceTestsBase(ITestOutputHelper testOutputHelper)
        {
            TestOutputHelper = testOutputHelper;
            _logCapture = CaptureLogs(testOutputHelper);
        }

        public async Task InitializeAsync()
        {
            _fixture = await CreateFixture();
        }

        protected IStreamStore store => _fixture.Store;

        protected IStreamStoreFixture fixture => _fixture;

        public Task DisposeAsync()
        {
            _fixture.Dispose();
            _logCapture.Dispose();
            return Task.CompletedTask;
        }

        protected abstract Task<IStreamStoreFixture> CreateFixture();

        private static IDisposable CaptureLogs(ITestOutputHelper testOutputHelper)
            => LoggingHelper.Capture(testOutputHelper);

        [Fact]
        public async Task When_dispose_and_read_then_should_throw()
        {
            store.Dispose();

            Func<Task> act = () => store.ReadAllForwards(Position.Start, 10);

            await act.ShouldThrowAsync<ObjectDisposedException>();
        }

        [Fact]
        public void Can_dispose_more_than_once()
        {
            store.Dispose();

            Action act = store.Dispose;

            act.ShouldNotThrow();
        }

        public static NewStreamMessage[] CreateNewStreamMessages(params int[] messageNumbers)
        {
            return CreateNewStreamMessages(DefaultJsonData, messageNumbers);
        }

        public static NewStreamMessage[] CreateNewStreamMessages(string jsonData, params int[] messageNumbers)
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
            return new StreamMessage(streamId,
                id,
                sequenceNumber,
                0,
                created,
                "type",
                DefaultJsonMetadata,
                DefaultJsonData);
        }

        protected static async Task AppendMessages(
            IStreamStore streamStore,
            string streamId,
            int numberOfEvents)
        {
            for(int i = 0; i < numberOfEvents; i++)
            {
                var newmessage = new NewStreamMessage(Guid.NewGuid(), "MyEvent", "{}");
                await streamStore.AppendToStream(streamId, ExpectedVersion.Any, newmessage);
            }
        }
    }
}