namespace SqlStreamStore
{
    using System;
    using System.Diagnostics;
    using System.Threading.Tasks;
    using Shouldly;
    using SqlStreamStore.Streams;
    using Xunit;
    using Xunit.Abstractions;

    public class MsSqlStreamStoreV3AcceptanceTests : AcceptanceTests, IClassFixture<FixturePool>
    {
        private readonly FixturePool _fixturePool;

        public MsSqlStreamStoreV3AcceptanceTests(FixturePool fixturePool, ITestOutputHelper testOutputHelper)
            : base(testOutputHelper)
        {
            _fixturePool = fixturePool;
        }

        protected override async Task<IStreamStoreFixture> CreateFixture() 
            => await _fixturePool.Get(TestOutputHelper);

        [Fact]
        public async Task Can_drop_all()
        {
            using (var fixture = await _fixturePool.Get(TestOutputHelper, "abc"))
            {
                await fixture.MsSqlStreamStoreV3.DropAll();
            }
        }

        [Fact]
        public async Task Can_check_schema()
        {
            using (var fixture = await _fixturePool.Get(TestOutputHelper))
            {
                var result = await fixture.MsSqlStreamStoreV3.CheckSchema();

                result.ExpectedVersion.ShouldBe(3);
                result.CurrentVersion.ShouldBe(3);
                result.IsMatch().ShouldBeTrue();
            }
        }

        [Fact]
        public async Task When_schema_is_not_created_then_should_be_indicated()
        {
            using (var fixture = await _fixturePool.Get(TestOutputHelper, "notcreated"))
            {
                await fixture.MsSqlStreamStoreV3.DropAll();

                var result = await fixture.MsSqlStreamStoreV3.CheckSchema();

                result.ExpectedVersion.ShouldBe(3);
                result.CurrentVersion.ShouldBe(0);
                result.IsMatch().ShouldBeFalse();
            }
        }

        [Fact]
        public void Can_export_database_creation_script()
        {
            const string schema = "custom_schema";
            var store = new MsSqlStreamStoreV3(new MsSqlStreamStoreV3Settings("server=.;database=sss")
            {
                Schema = schema,
            });

            var sqlScript = store.GetSchemaCreationScript();
            
            sqlScript.ShouldBe(new ScriptsV3.Scripts("custom_schema").CreateSchema);
        }

        [Fact]
        public async Task Time_taken_to_append_and_read_large_message()
        {
            var stopwatch = Stopwatch.StartNew();
            var streamId = "stream-large";
            var data = new string('a', 1024 * 1024 * 2);
            var newStreamMessage = new NewStreamMessage(Guid.NewGuid(), "foo", data);
            await Fixture.Store.AppendToStream(streamId, ExpectedVersion.Any, newStreamMessage);
            TestOutputHelper.WriteLine($"Append: {stopwatch.Elapsed}");

            stopwatch.Restart();
            var readStreamPage = await Fixture.Store.ReadStreamForwards(streamId, StreamVersion.Start, 1);
            var jsonData = await readStreamPage.Messages[0].GetJsonData();
            TestOutputHelper.WriteLine($"Read: {stopwatch.Elapsed}");
        }

        [Fact]
        public async Task Time_taken_to_append_and_read_large_message_without_prefetch()
        {
            var stopwatch = Stopwatch.StartNew();
            var streamId = "stream-large";
            var data = new string('a', 1024 * 1024 * 2);
            var newStreamMessage = new NewStreamMessage(Guid.NewGuid(), "foo", data);
            await Fixture.Store.AppendToStream(streamId, ExpectedVersion.Any, newStreamMessage);
            TestOutputHelper.WriteLine($"Append: {stopwatch.Elapsed}");

            stopwatch.Restart();
            var readStreamPage = await Fixture.Store.ReadStreamForwards(streamId, StreamVersion.Start, prefetchJsonData: false, maxCount: 1);
            var jsonData = await readStreamPage.Messages[0].GetJsonData();
            TestOutputHelper.WriteLine($"Read: {stopwatch.Elapsed}");
        }
    }
}