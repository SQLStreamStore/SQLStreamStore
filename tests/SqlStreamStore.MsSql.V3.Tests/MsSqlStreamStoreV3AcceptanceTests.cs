namespace SqlStreamStore
{
    using System;
    using System.Diagnostics;
    using System.Threading.Tasks;
    using Microsoft.Data.SqlClient;
    using Shouldly;
    using SqlStreamStore.Streams;
    using Xunit;
    using Xunit.Abstractions;

    public class MsSqlStreamStoreV3AcceptanceTests : AcceptanceTests, IClassFixture<MsSqlStreamStoreV3FixturePool>
    {
        private readonly MsSqlStreamStoreV3FixturePool _fixturePool;

        public MsSqlStreamStoreV3AcceptanceTests(MsSqlStreamStoreV3FixturePool fixturePool, ITestOutputHelper testOutputHelper)
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
        [Fact]
        public async Task Can_Not_Read_When_No_Rights()
        {
            using (var fixture = await _fixturePool.Get(TestOutputHelper))
            {
                await fixture.CreateUser("can_not_read");
                var streamId = Guid.NewGuid().ToString();

                var newStreamMessage = CreateNewStreamMessages(1);
                await fixture.Store.AppendToStream(streamId, ExpectedVersion.Any, newStreamMessage);

                var store = fixture.StoreFor("can_not_read");
                var exception = await Record.ExceptionAsync(() => store.ReadStreamForwards(streamId, StreamVersion.Start, 1));
                exception.ShouldBeOfType<SqlException>()
                    .Message.ShouldContain("The SELECT permission was denied");
            }
        }
        [Fact]
        public async Task Can_Not_Read_When_Rights()
        {
            using (var fixture = await _fixturePool.Get(TestOutputHelper))
            {
                await fixture.CreateUser("can_read");
                await fixture.MsSqlStreamStoreV3.GrantRead("can_read");

                var streamId = Guid.NewGuid().ToString();
                var newStreamMessage = CreateNewStreamMessages(1);
                await fixture.Store.AppendToStream(streamId, ExpectedVersion.Any, newStreamMessage);

                var store = fixture.StoreFor("can_read");
                var exception = await Record.ExceptionAsync(() => store.ReadStreamForwards(streamId, StreamVersion.Start, 1));
                exception.ShouldBeNull();
            }
        }

        [Fact]
        public async Task Can_Not_Append_When_No_Rights()
        {
            using (var fixture = await _fixturePool.Get(TestOutputHelper))
            {
                await fixture.CreateUser("can_not_append");
                var store = fixture.StoreFor("can_not_append");

                var streamId = Guid.NewGuid().ToString();
                var newStreamMessage = CreateNewStreamMessages(1);

                var exception = await Record.ExceptionAsync(() => store.AppendToStream(streamId, ExpectedVersion.Any, newStreamMessage));
                exception.ShouldBeOfType<SqlException>()
                    .Message.ShouldStartWith("The EXECUTE permission was denied on the object 'NewStreamMessages'");
            }
        }
        [Fact]
        public async Task Can_Append_When_Rights()
        {
            using (var fixture = await _fixturePool.Get(TestOutputHelper))
            {
                await fixture.CreateUser("can_append");
                await fixture.MsSqlStreamStoreV3.GrantAppend("can_append");

                var store = fixture.StoreFor("can_append");

                var streamId = Guid.NewGuid().ToString();
                var newStreamMessage = CreateNewStreamMessages(1);

                var exception = await Record.ExceptionAsync(() =>
                    store.AppendToStream(streamId, ExpectedVersion.Any, newStreamMessage));
                exception.ShouldBeNull();
            }
        }
    }
}