namespace Cedar.EventStore
{
    using System.Threading.Tasks;
    using Cedar.EventStore.Streams;
    using Shouldly;
    using Xunit;

    public partial class EventStoreAcceptanceTests
    {
        private EventStoreAcceptanceTestFixture GetFixture(string schema = "foo")
        {
            return new MsSqlEventStoreFixture(schema);
        }

        [Fact]
        public async Task Can_use_multiple_schemas()
        {
            using(var fixture = new MsSqlEventStoreFixture("dbo"))
            {
                using(var dboEventStore = await fixture.GetEventStore())
                {
                    using(var barEventStore = await fixture.GetEventStore("bar"))
                    {
                        await dboEventStore.AppendToStream("stream-1",
                                ExpectedVersion.NoStream,
                                CreateNewStreamEvents(1, 2));
                        await barEventStore.AppendToStream("stream-1",
                                ExpectedVersion.NoStream,
                                CreateNewStreamEvents(1, 2));

                        var dboHeadCheckpoint = await dboEventStore.ReadHeadCheckpoint();
                        var fooHeadCheckpoint = await dboEventStore.ReadHeadCheckpoint();

                        dboHeadCheckpoint.ShouldBe(1);
                        fooHeadCheckpoint.ShouldBe(1);

                    }
                }
            }
        }
    }
}