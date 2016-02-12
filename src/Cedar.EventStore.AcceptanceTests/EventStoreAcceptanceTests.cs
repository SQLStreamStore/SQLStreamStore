namespace Cedar.EventStore
{
    using System;
    using System.Diagnostics;
    using System.Linq;
    using System.Threading.Tasks;
    using Cedar.EventStore.Streams;
    using Shouldly;
    using Xunit;
    using Xunit.Abstractions;
    using Timer = System.Threading.Timer;

    public partial class EventStoreAcceptanceTests
    {
        private readonly ITestOutputHelper _testOutputHelper;

        

        public EventStoreAcceptanceTests(ITestOutputHelper testOutputHelper)
        {
            _testOutputHelper = testOutputHelper;
        }

        [Fact]
        public async Task When_dispose_and_read_then_should_throw()
        {
            using(var fixture = GetFixture())
            {
                var store = await fixture.GetEventStore();
                store.Dispose();

                Func<Task> act = () => store.ReadAll(Checkpoint.Start, 10);

                act.ShouldThrow<ObjectDisposedException>();
            }
        }

        [Fact]
        public async Task Can_dispose_more_than_once()
        {
            using (var fixture = GetFixture())
            {
                var store = await fixture.GetEventStore();
                store.Dispose();

                Action act = store.Dispose;

                act.ShouldNotThrow();
            }
        }

        private static NewStreamEvent[] CreateNewStreamEvents(params int[] eventNumbers)
        {
            return eventNumbers
                .Select(eventNumber =>
                {
                    var eventId = Guid.Parse("00000000-0000-0000-0000-" + eventNumber.ToString().PadLeft(12, '0'));
                    return new NewStreamEvent(eventId, "type", "\"data\"", "\"metadata\"");
                })
                .ToArray();
        }

        private static StreamEvent ExpectedStreamEvent(
            string streamId,
            int eventNumber,
            int sequenceNumber,
            DateTime created)
        {
            var eventId = Guid.Parse("00000000-0000-0000-0000-" + eventNumber.ToString().PadLeft(12, '0'));
            return new StreamEvent(streamId, eventId, sequenceNumber, 0, created, "type", "\"data\"", "\"metadata\"");
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
    }

    public class ResettableTimeout<T>
    {
        private TaskCompletionSource<T> _taskCompletionSource = new TaskCompletionSource<T>();

        public ResettableTimeout(int timeout)
        {
            var timer = new Timer(_ => { });

            T	
        }
    }
}