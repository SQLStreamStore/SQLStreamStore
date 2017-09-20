namespace SqlStreamStore
{
    using System;
    using System.Threading.Tasks;
    using Shouldly;
    using SqlStreamStore.Streams;
    using Xunit;
    using Xunit.Abstractions;

    public class MySqlStreamStoreAcceptanceTests : StreamStoreAcceptanceTests
    {
        private readonly ITestOutputHelper _testOutputHelper;

        public MySqlStreamStoreAcceptanceTests(ITestOutputHelper testOutputHelper)
            : base(testOutputHelper)
        {
            _testOutputHelper = testOutputHelper;
        }

        protected override StreamStoreAcceptanceTestFixture GetFixture() => new MySqlStreamStoreFixture(_testOutputHelper);

        [Fact]
        public async Task Can_get_stream_message_count_with_created_before_date()
        {
            using (var fixture = new MySqlStreamStoreFixture(_testOutputHelper))
            {
                using (var store = await fixture.GetMySqlStreamStore())
                {
                    fixture.GetUtcNow = () => new DateTime(2016, 1, 1, 0, 0, 0);

                    var streamId = "stream-1";
                    await store.AppendToStream(
                        streamId,
                        ExpectedVersion.NoStream,
                        CreateNewStreamMessages(1, 2, 3));

                    fixture.GetUtcNow = () => new DateTime(2016, 1, 1, 0, 1, 0);

                    await store.AppendToStream(
                        streamId,
                        ExpectedVersion.Any,
                        CreateNewStreamMessages(4, 5, 6));

                    var streamCount = await store.GetMessageCount(streamId, new DateTime(2016, 1, 1, 0, 1, 0));

                    streamCount.ShouldBe(3); // The first 3
                }
            }
        }

        [Fact]
        public async Task Can_call_initialize_repeatably()
        {
            using(var fixture = new MySqlStreamStoreFixture(_testOutputHelper))
            {
                using(var store = await fixture.GetMySqlStreamStore())
                {
                    await store.CreateDatabase();
                    await store.CreateDatabase();
                }
            }
        }

        [Fact]
        public async Task Can_drop_all()
        {
            using (var fixture = new MySqlStreamStoreFixture(_testOutputHelper))
            {
                using (var store = await fixture.GetMySqlStreamStore())
                {
                    await store.DropAll();
                }
            }
        }
    }
}