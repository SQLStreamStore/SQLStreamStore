﻿namespace SqlStreamStore
{
    using System;
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.Linq;
    using System.Threading.Tasks;
    using Shouldly;
    using SqlStreamStore.Streams;
    using Xunit;
    using Xunit.Abstractions;

    public abstract partial class StreamStoreAcceptanceTests : IDisposable
    {
        private readonly ITestOutputHelper _testOutputHelper;
        private readonly IDisposable _logCapture;
        protected readonly StreamStoreAcceptanceTestFixture fixture;
        protected readonly IStreamStore store;

        public StreamStoreAcceptanceTests(ITestOutputHelper testOutputHelper)
        {
            _testOutputHelper = testOutputHelper;
            _logCapture = CaptureLogs(testOutputHelper);
            fixture = GetFixture();
            store = fixture.GetStreamStore().Result;
        }

        public void Dispose()
        {
            store.Dispose();
            fixture.Dispose();
            _logCapture.Dispose();
        }

        protected abstract StreamStoreAcceptanceTestFixture GetFixture();

        protected abstract IDisposable CaptureLogs(ITestOutputHelper testOutputHelper);

        [Fact]
        public async Task When_dispose_and_read_then_should_throw()
        {
            using(var fixture = GetFixture())
            {
                var store = await fixture.GetStreamStore();
                store.Dispose();

                Func<Task> act = () => store.ReadAllForwards(Position.Start, 10);

                act.ShouldThrow<ObjectDisposedException>();
            }
        }

        [Fact]
        public async Task Can_dispose_more_than_once()
        {
            using (var fixture = GetFixture())
            {
                var store = await fixture.GetStreamStore();
                store.Dispose();

                Action act = store.Dispose;

                act.ShouldNotThrow();
            }
        }

        public static NewStreamMessage[] CreateNewStreamMessages(params int[] messageNumbers)
        {
            return CreateNewStreamMessages("\"data\"", messageNumbers);
        }

        public static NewStreamMessage[] CreateNewStreamMessages(string jsonData, params int[] messageNumbers)
        {
            return messageNumbers
                .Select(number =>
                {
                    var id = Guid.Parse("00000000-0000-0000-0000-" + number.ToString().PadLeft(12, '0'));
                    return new NewStreamMessage(id, "type", jsonData, "\"metadata\"");
                })
                .ToArray();
        }

        private static NewStreamMessage[] CreateNewStreamMessageSequence(int startId, int count)
        {
            var messages = new List<NewStreamMessage>();
            for(int i = 0; i < count; i++)
            {
                var mwssageNumber = startId + i;
                var messageId = Guid.Parse("00000000-0000-0000-0000-" + mwssageNumber.ToString().PadLeft(12, '0'));
                var newStreamMessage = new NewStreamMessage(messageId, "type", "\"data\"", "\"metadata\"");
                messages.Add(newStreamMessage);
            }
            return messages.ToArray();
        }

        private static StreamMessage ExpectedStreamMessage(
            string streamId,
            int messageNumber,
            int sequenceNumber,
            DateTime created)
        {
            var id = Guid.Parse("00000000-0000-0000-0000-" + messageNumber.ToString().PadLeft(12, '0'));
            return new StreamMessage(streamId, id, sequenceNumber, 0, created, "type", "\"metadata\"", "\"data\"");
        }
    }

    public static class TaskExtensions
    {
        private static Func<int, int> TaskTimeout => timeout => Debugger.IsAttached ? 30000 : timeout;

        public static async Task<T> WithTimeout<T>(this Task<T> task, int timeout = 3000)
        {
            if (await Task.WhenAny(task, Task.Delay(timeout)) == task)
            {
                return await task;
            }
            throw new TimeoutException("Timed out waiting for task");
        }

        public static async Task WithTimeout(this Task task, int timeout = 3000)
        {
            if (await Task.WhenAny(task, Task.Delay(timeout)) == task)
            {
                return;
            }
            throw new TimeoutException("Timed out waiting for task");
        }
    }
}
