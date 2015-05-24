namespace Cedar.EventStore
{
    using System.Threading.Tasks;
    using Cedar.EventStore.Exceptions;
    using Xunit;

    public abstract partial class EventStoreAcceptanceTests
    {
        [Fact]
        public async Task When_append_stream_second_time_with_no_stream_expected_then_should_throw()
        {
            using(var fixture = GetFixture())
            {
                using(var eventStore = await fixture.GetEventStore())
                {
                    await eventStore
                        .AppendToStream("stream-1", ExpectedVersion.NoStream, CreateNewStreamEvents(1, 2, 3));

                    await eventStore
                        .AppendToStream("stream-1", ExpectedVersion.NoStream, CreateNewStreamEvents(4, 5, 6))
                        .ShouldThrow<WrongExpectedVersionException>();
                }
            }
        }

        [Fact]
        public async Task When_append_with_wrong_expected_version_then_should_throw()
        {
            using (var fixture = GetFixture())
            {
                using (var eventStore = await fixture.GetEventStore())
                {
                    await eventStore
                        .AppendToStream("stream-1", ExpectedVersion.NoStream, CreateNewStreamEvents(1, 2, 3));

                    await eventStore
                        .AppendToStream("stream-1", 1, CreateNewStreamEvents(4, 5, 6))
                        .ShouldThrow<WrongExpectedVersionException>();
                }
            }
        }
    }
}
