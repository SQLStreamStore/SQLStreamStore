namespace SqlStreamStore
{
    using System.Threading.Tasks;
    using Shouldly;
    using SqlStreamStore.Streams;
    using Xunit;
    using Xunit.Abstractions;

    public class MsSqlStreamStoreV3AcceptanceTests : AcceptanceTests
    {
        public MsSqlStreamStoreV3AcceptanceTests(ITestOutputHelper testOutputHelper)
            : base(testOutputHelper)
        { }

        protected override StreamStoreAcceptanceTestFixture GetFixture()
            => new MsSqlStreamStoreV3Fixture("foo");

        protected override async Task<IStreamStoreFixture> CreateFixture()
            => await MsSqlStreamStoreV3Fixture2.Create("foo");

        [Fact]
        public async Task Can_use_multiple_schemas()
        {
            var dboStore = fixture.Store;

            using (var barFixture = await MsSqlStreamStoreV3Fixture2.Create("bar"))
            {
                var barStore = barFixture.Store;

                await dboStore.AppendToStream("stream-1",
                    ExpectedVersion.NoStream,
                    CreateNewStreamMessages(1, 2));
                await barStore.AppendToStream("stream-1",
                    ExpectedVersion.NoStream,
                    CreateNewStreamMessages(1, 2));

                var dboHeadPosition = await dboStore.ReadHeadPosition();
                var barHeadPosition = await barStore.ReadHeadPosition();

                dboHeadPosition.ShouldBe(1);
                barHeadPosition.ShouldBe(1);
            }
        }

        [Theory, InlineData("dbo"), InlineData("myschema")]
        public async Task Can_call_initialize_repeatably(string schema)
        {
            using (var fixture = await MsSqlStreamStoreV3Fixture2.CreateUninitialized(schema))
            {
                await fixture.Store.CreateSchemaIfNotExists();
                await fixture.Store.CreateSchemaIfNotExists();
            }
        }

        [Fact]
        public async Task Can_drop_all()
        {
            using (var fixture = await MsSqlStreamStoreV3Fixture2.Create())
            {
                await fixture.Store.DropAll();
            }
        }

        [Fact]
        public async Task Can_check_schema()
        {
            using (var fixture = await MsSqlStreamStoreV3Fixture2.Create())
            {
                var result = await fixture.Store.CheckSchema();

                result.ExpectedVersion.ShouldBe(3);
                result.CurrentVersion.ShouldBe(3);
                result.IsMatch().ShouldBeTrue();
            }
        }

        [Fact]
        public async Task When_schema_is_not_created_then_should_be_indicated()
        {
            using (var fixture = await MsSqlStreamStoreV3Fixture2.CreateUninitialized())
            {
                var result = await fixture.Store.CheckSchema();

                result.ExpectedVersion.ShouldBe(3);
                result.CurrentVersion.ShouldBe(0);
                result.IsMatch().ShouldBeFalse();
            }
        }

        [Fact]
        public void Can_export_database_creation_script()
        {
            string schema = "custom_schema";
            var store = new MsSqlStreamStoreV3(new MsSqlStreamStoreV3Settings("server=.;database=sss")
            {
                Schema = schema,
            });


            var sqlScript = store.GetSchemaCreationScript();
            sqlScript.ShouldBe(new ScriptsV3.Scripts("custom_schema").CreateSchema);
        }
    }
}