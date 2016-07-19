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
            using(var scavenger = await CreateScavenger())
            {
                var checkpoint = scavenger.GetCheckpoint();

                checkpoint.ShouldBeNull();
            }
        }

        [Fact]
        public async Task When_received_event_then_should_record_event_expiry_as_datetime_max()
        {
            using(var scavenger = await CreateScavenger())
            {
                var streamId = "stream-1";
                var newStreamEvent = new NewStreamEvent(Guid.NewGuid(), "type", "json");
                var streamEventProcessed = scavenger
                    .WaitForStreamEventProcessed(@event => @event.EventId == newStreamEvent.EventId);
                await _store.AppendToStream(streamId, ExpectedVersion.NoStream, newStreamEvent);

                await streamEventProcessed;

                var scavengerStreamEvent = scavenger.GetStreamEvent(streamId, newStreamEvent.EventId);
                scavengerStreamEvent.Expires.ShouldBe(DateTime.MaxValue);
            }
        }

        [Fact]
        public async Task When_received_event_then_should_update_checkpoint()
        {
            using(var scavenger = await CreateScavenger())
            {
                var streamId = "stream-1";
                var newStreamEvent = new NewStreamEvent(Guid.NewGuid(), "type", "json");
                var streamEventProcessed = scavenger
                    .WaitForStreamEventProcessed(@event => @event.EventId == newStreamEvent.EventId);
                await _store.AppendToStream(streamId, ExpectedVersion.NoStream, newStreamEvent);

                var streamEvent = await streamEventProcessed;

                var checkpoint = scavenger.GetCheckpoint();
                checkpoint.ShouldBe(streamEvent.Checkpoint);
            }
        }

        [Fact]
        public async Task When_stream_metadata_set_with_max_age_then_then_should_update_expiries()
        {
            using(var scavenger = await CreateScavenger())
            {
                var streamId = "stream-1";
                var newStreamEvent = new NewStreamEvent(Guid.NewGuid(), "type", "json");
                await _store.AppendToStream(streamId, ExpectedVersion.NoStream, newStreamEvent);
                var streamEventProcessed = scavenger
                    .WaitForStreamEventProcessed(@event => @event.StreamId == $"$${streamId}");

                await _store.SetStreamMetadata(streamId, maxAge: 360);

                await streamEventProcessed;
                var scavengerStreamEvent = scavenger.GetStreamEvent(streamId, newStreamEvent.EventId);
                scavengerStreamEvent.Expires.ShouldBeLessThan(DateTime.MaxValue);
                scavengerStreamEvent.Expires.ShouldBeGreaterThan(_getUtcNow().DateTime);
            }
        }

        [Fact]
        public async Task When_stream_metadata_set_first_with_max_age_then_then_should_have_correct_expiry()
        {
            using(var scavenger = await CreateScavenger())
            {
                var streamId = "stream-1";
                var newStreamEvent = new NewStreamEvent(Guid.NewGuid(), "type", "json");
                await _store.SetStreamMetadata(streamId, maxAge: 360);
                var streamEventProcessed = scavenger
                    .WaitForStreamEventProcessed(@event => @event.EventId == newStreamEvent.EventId);
                await _store.AppendToStream(streamId, ExpectedVersion.NoStream, newStreamEvent);

                await streamEventProcessed;

                var scavengerStreamEvent = scavenger.GetStreamEvent(streamId, newStreamEvent.EventId);
                scavengerStreamEvent.Expires.ShouldBe(_getUtcNow().DateTime.AddSeconds(360));
            }
        }

        [Fact]
        public async Task When_stream_metadata_set_second_time_with_max_age_then_then_should_have_correct_expiry()
        {
            // Arrange
            using(var scavenger = await CreateScavenger())
            {
                var streamId = "stream-1";
                var eventId = Guid.NewGuid();

                await _store.SetStreamMetadata(streamId, maxAge: 360);

                var streamEventProcessed = scavenger
                    .WaitForStreamEventProcessed(@event => @event.EventId == eventId);

                var newStreamEvent = new NewStreamEvent(eventId, "type", "json");
                await _store.AppendToStream(streamId, ExpectedVersion.NoStream, newStreamEvent);
                await streamEventProcessed;
                streamEventProcessed = scavenger
                    .WaitForStreamEventProcessed(@event => @event.StreamId == $"$${streamId}");

                // Act
                await _store.SetStreamMetadata(streamId, maxAge: 720);

                // Assert
                await streamEventProcessed;
                var scavengerStreamEvent = scavenger.GetStreamEvent(streamId, newStreamEvent.EventId);
                scavengerStreamEvent.Expires.ShouldBe(_getUtcNow().DateTime.AddSeconds(720));
            }
        }

        [Fact]
        public async Task When_scavange_then_should_delete_expired_event()
        {
            using(var scavenger = await CreateScavenger(scavangeInterval: 1))
            {
                // Arrange
                var streamId = "stream-1";
                var newStreamEvent = new NewStreamEvent(Guid.NewGuid(), "type", "json");
                var metadataProcessed = scavenger
                    .WaitForStreamEventProcessed(@event => @event.StreamId == $"$${streamId}");
                var eventDeletedProcessed = scavenger
                    .WaitForStreamEventProcessed(
                        @event => @event.StreamId == Deleted.DeletedStreamId 
                        && @event.Type == Deleted.EventDeletedEventType);
                await _store.AppendToStream(streamId, ExpectedVersion.NoStream, newStreamEvent);
                await _store.SetStreamMetadata(streamId, maxAge: 1);
                await metadataProcessed;

                // Act
                _utcNow = _utcNow.AddMinutes(1);
                var streamEvent = await eventDeletedProcessed;

                // Assert
                streamEvent
                    .JsonDataAs<Deleted.EventDeleted>()
                    .StreamId
                    .ShouldBe(streamId);
                scavenger
                    .GetStreamEvent(streamId, newStreamEvent.EventId)
                    .ShouldBeNull();
            }
        }


        [Fact]
        public async Task When_stream_deleted_then_should_remove_from_index()
        {
            using (var scavenger = await CreateScavenger(scavangeInterval: 1))
            {
                // Arrange
                var streamId = "stream-1";
                var newStreamEvent = new NewStreamEvent(Guid.NewGuid(), "type", "json");

                var eventCreatedProcessed = scavenger
                    .WaitForStreamEventProcessed(
                        @event => @event.StreamId == streamId);

                var eventDeletedProcessed = scavenger
                    .WaitForStreamEventProcessed(
                        @event => @event.StreamId == Deleted.DeletedStreamId
                        && @event.Type == Deleted.StreamDeletedEventType);

                await _store.AppendToStream(streamId, ExpectedVersion.NoStream, newStreamEvent);
                await eventCreatedProcessed;

                // Act
                await _store.DeleteStream(streamId);
                await eventDeletedProcessed;

                // Assert
                var streamEvent = await eventDeletedProcessed;

                streamEvent
                    .JsonDataAs<Deleted.StreamDeleted>()
                    .StreamId
                    .ShouldBe(streamId);
                scavenger
                    .GetStream(streamId)
                    .ShouldBeNull();
            }
        }

        [Fact]
        public async Task When_underlying_event_store_is_disposed()
        {
            using (var scavenger = await CreateScavenger(scavangeInterval: 1))
            {
                // Arrange
                var streamId = "stream-1";
                var newStreamEvent = new NewStreamEvent(Guid.NewGuid(), "type", "json");
                var metadataProcessed = scavenger
                    .WaitForStreamEventProcessed(@event => @event.StreamId == $"$${streamId}");
                var eventDeletedProcessed = scavenger
                    .WaitForStreamEventProcessed(
                        @event => @event.StreamId == Deleted.DeletedStreamId
                        && @event.Type == Deleted.EventDeletedEventType);
                await _store.AppendToStream(streamId, ExpectedVersion.NoStream, newStreamEvent);
                await _store.SetStreamMetadata(streamId, maxAge: 1);
                await metadataProcessed;

                // Act
                _utcNow = _utcNow.AddMinutes(1);
                _store.Dispose();
                await scavenger.ScavangeNow();
            }
        }

        private async Task<InMemoryScavenger> CreateScavenger(int scavangeInterval = 60000)
        {
            var scavenger = new InMemoryScavenger(_store, _getUtcNow, scavangeInterval: scavangeInterval);
            await scavenger.Initialize();
            return scavenger;
        }

        public void Dispose()
        {
            _store.Dispose();
        }
    }
}