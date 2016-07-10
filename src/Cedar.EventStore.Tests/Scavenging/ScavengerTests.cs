namespace Cedar.EventStore.Scavenging
{
    using System;
    using System.IO;
    using System.Threading.Tasks;
    using Cedar.EventStore.Streams;
    using Shouldly;
    using Xunit;

    public class ScavengerTests : IDisposable
    {
        private readonly InMemoryEventStore _store;
        private readonly DirectoryInfo _directoryInfo = new DirectoryInfo($"scavenger\\{Guid.NewGuid()}");

        public ScavengerTests()
        {
            _store = new InMemoryEventStore();
        }

        [Fact]
        public async Task Sqlite_schema_version_should_be_zero()
        {
            var scavenger = new Scavenger(_store, _directoryInfo);

            (await scavenger.GetSchemaVersion()).ShouldBe(1);

            scavenger.Complete();
            await scavenger.Completion;
        }

        [Fact]
        public async Task When_schema_mismatch_then_db_should_be_reset()
        {
            var scavenger = new Scavenger(_store, _directoryInfo);
            scavenger.Complete();
            await scavenger.Completion;
            scavenger = new Scavenger(_store, _directoryInfo, Scavenger.SqliteSchemaVersion - 1);
            int schemaVersion = await scavenger.GetSchemaVersion();

            schemaVersion.ShouldBe(Scavenger.SqliteSchemaVersion);

            scavenger.Complete();
            await scavenger.Completion;
        }

        [Fact]
        public async Task When_event_handled_then_should_raise_stream_event_handled()
        {
            var streamId = "stream-1";
            var scavenger = new Scavenger(_store, _directoryInfo);
            var tcs = new TaskCompletionSource<StreamEvent>();
            scavenger.StreamEventHandled += (sender, @event) =>
            {
                tcs.SetResult(@event);
            };

            var newStreamEvent = new NewStreamEvent(Guid.NewGuid(), "event-type", "data");
            await _store.AppendToStream(streamId, ExpectedVersion.NoStream, newStreamEvent);

            var streamEvent = await tcs.Task;
            streamEvent.EventId.ShouldBe(newStreamEvent.EventId);
        }

        [Fact]
        public void When_scavenger_is_marked_complete_then_further_operations_should_throw()
        {
            var scavenger = new Scavenger(_store, _directoryInfo);
            scavenger.Complete();

            Action act = () => scavenger.GetSchemaVersion();
            
            act.ShouldThrow<TaskSchedulerException>();
        }

        [Fact]
        public async Task Initial_checpoint_should_be_null()
        {
            var scavenger = new Scavenger(_store, _directoryInfo);

            var checkpoint = await scavenger.GetCheckpoint();

            checkpoint.HasValue.ShouldBeFalse();

            scavenger.Complete();
        }

        public void Dispose()
        {
            _store.Dispose();
        }
    }
}
