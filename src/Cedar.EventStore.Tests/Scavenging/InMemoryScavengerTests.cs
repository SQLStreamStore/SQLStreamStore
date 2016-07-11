namespace Cedar.EventStore.Scavenging
{
    using System;
    using System.Threading.Tasks;
    using Cedar.EventStore.Infrastructure;
    using Cedar.EventStore.Streams;
    using Shouldly;
    using Xunit;

    public class InMemoryScavengerTests : IDisposable
    {
        private readonly IEventStore _store;
        private DateTime _utcNow = new DateTime(2016, 1, 1);
        private readonly GetUtcNow _getUtcNow;

        public InMemoryScavengerTests()
        {
            _getUtcNow = () => _utcNow;
            _store = new InMemoryEventStore(_getUtcNow);
        }

        [Fact]
        public async Task Start_checkpoint_should_be_null()
        {
            var scavenger = await CreateScavenger();
            var checkpoint =  await scavenger.GetCheckpoint();

            checkpoint.ShouldBeNull();

            await scavenger.Complete();
        }

        [Fact]
        public async Task When_received_event_then_should_record_event_expiry_as_datetime_max()
        {
            var scavenger = await CreateScavenger();
            var streamId = "stream-1";
            var newStreamEvent = new NewStreamEvent(Guid.NewGuid(), "type", "json");
            var streamEventProcessed = scavenger
                .WaitForStreamEventProcessed(@event => @event.EventId == newStreamEvent.EventId);
            await _store.AppendToStream(streamId, ExpectedVersion.NoStream, newStreamEvent);

            await streamEventProcessed;

            var scavengerStreamEvent = await scavenger.GetStreamEvent(streamId, newStreamEvent.EventId);
            scavengerStreamEvent.Expires.ShouldBe(DateTime.MaxValue);

            await scavenger.Complete();
        }

        [Fact]
        public async Task When_received_event_then_should_update_checkpoint()
        {
            var scavenger = await CreateScavenger();
            var streamId = "stream-1";
            var newStreamEvent = new NewStreamEvent(Guid.NewGuid(), "type", "json");
            var streamEventProcessed = scavenger
                .WaitForStreamEventProcessed(@event => @event.EventId == newStreamEvent.EventId);
            await _store.AppendToStream(streamId, ExpectedVersion.NoStream, newStreamEvent);

            var streamEvent = await streamEventProcessed;

            var checkpoint = await scavenger.GetCheckpoint();
            checkpoint.ShouldBe(streamEvent.Checkpoint);

            await scavenger.Complete();
        }

        [Fact]
        public async Task When_stream_metadata_set_with_max_age_then_then_should_update_expiries()
        {
            var scavenger = await CreateScavenger();
            var streamId = "stream-1";
            var newStreamEvent = new NewStreamEvent(Guid.NewGuid(), "type", "json");
            await _store.AppendToStream(streamId, ExpectedVersion.NoStream, newStreamEvent);
            var streamEventProcessed = scavenger
                .WaitForStreamEventProcessed(@event => @event.StreamId == $"$${streamId}");

            await _store.SetStreamMetadata(streamId, maxAge: 360);

            await streamEventProcessed;
            var scavengerStreamEvent = await scavenger.GetStreamEvent(streamId, newStreamEvent.EventId);
            scavengerStreamEvent.Expires.ShouldBeLessThan(DateTime.MaxValue);
            scavengerStreamEvent.Expires.ShouldBeGreaterThan(_getUtcNow().DateTime);

            await scavenger.Complete();
        }

        [Fact]
        public async Task When_stream_metadata_set_first_with_max_age_then_then_should_have_correct_expiry()
        {
            var scavenger = await CreateScavenger();
            var streamId = "stream-1";
            var newStreamEvent = new NewStreamEvent(Guid.NewGuid(), "type", "json");
            await _store.SetStreamMetadata(streamId, maxAge: 360);
            var streamEventProcessed = scavenger
                .WaitForStreamEventProcessed(@event => @event.EventId == newStreamEvent.EventId);
            await _store.AppendToStream(streamId, ExpectedVersion.NoStream, newStreamEvent);
            
            await streamEventProcessed;

            var scavengerStreamEvent = await scavenger.GetStreamEvent(streamId, newStreamEvent.EventId);
            scavengerStreamEvent.Expires.ShouldBe(_getUtcNow().DateTime.AddSeconds(360));

            await scavenger.Complete();
        }

        [Fact]
        public async Task When_stream_metadata_set_second_time_with_max_age_then_then_should_have_correct_expiry()
        {
            var scavenger = await CreateScavenger();
            var streamId = "stream-1";
            var newStreamEvent = new NewStreamEvent(Guid.NewGuid(), "type", "json");
            await _store.SetStreamMetadata(streamId, maxAge: 360);
            var streamEventProcessed = scavenger
               .WaitForStreamEventProcessed(@event => @event.EventId == newStreamEvent.EventId);
            await _store.AppendToStream(streamId, ExpectedVersion.NoStream, newStreamEvent);
            await streamEventProcessed;
            streamEventProcessed = scavenger
                .WaitForStreamEventProcessed(@event => @event.StreamId == $"$${streamId}");
            await _store.SetStreamMetadata(streamId, maxAge: 720);
            await streamEventProcessed;

            var scavengerStreamEvent = await scavenger.GetStreamEvent(streamId, newStreamEvent.EventId);
            scavengerStreamEvent.Expires.ShouldBe(_getUtcNow().DateTime.AddSeconds(720));

            await scavenger.Complete();
        }

        private async Task<InMemoryScavenger> CreateScavenger()
        {
            var scavenger = new InMemoryScavenger(_store, _getUtcNow);
            await scavenger.Initialize();
            return scavenger;
        }

        public void Dispose()
        {
            _store.Dispose();
        }
    }

    internal static class InMemoryScavengerExtensions
    {
        internal static Task<StreamEvent> WaitForStreamEventProcessed(
            this InMemoryScavenger scavenger,
            Predicate<StreamEvent> predicate = null)
        {
            predicate = predicate ?? (_ => true);
            var tcs = new TaskCompletionSource<StreamEvent>();
            EventHandler<StreamEvent> handler = (sender, streamEvent) =>
            {
                if(predicate(streamEvent))
                {
                    tcs.SetResult(streamEvent);
                }
            };
            scavenger.StreamEventProcessed += handler;
            return tcs.Task;
        }
    }
}