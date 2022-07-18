namespace SqlStreamStore
{
    using System.Threading.Tasks;
    using Shouldly;
    using SqlStreamStore.PgSqlScripts;
    using Xunit;
    using Xunit.Abstractions;

    public class PostgresStreamStoreAcceptanceTests : AcceptanceTests, IClassFixture<PostgresStreamStoreV3FixturePool>
    {
        private readonly PostgresStreamStoreV3FixturePool _fixturePool;

        public PostgresStreamStoreAcceptanceTests(
            PostgresStreamStoreV3FixturePool fixturePool,
            ITestOutputHelper testOutputHelper)
            : base(testOutputHelper)
        {
            _fixturePool = fixturePool;
        }

        protected override async Task<IStreamStoreFixture> CreateFixture()
            => await _fixturePool.Get(TestOutputHelper);

        [Fact]
        public async Task Can_check_schema()
        {
            using(var fixture = await _fixturePool.Get(TestOutputHelper))
            {
                var result = await fixture.PostgresStreamStore.CheckSchema();

                result.ShouldBe(new CheckSchemaResult(2, 2));
                result.IsMatch.ShouldBeTrue();
            }
        }

        [Fact]
        public async Task Can_export_database_migration_script()
        {
            var schema = "custom_schema";
            using(var fixture = await _fixturePool.Get(TestOutputHelper, schema))
            {
                var sqlScript = fixture.PostgresStreamStore.GetMigrationScript();
                sqlScript.ShouldBe(new Scripts(schema).Migration);
            }
        }

        /*
        [Fact]
        public async Task Can_use_multiple_schemas()
        {
            using (var dboFixture = await CreateFixture())
            {
                var dboStore = dboFixture.Store;

                using(var barFixture = await CreateFixture())
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
        }

        [Fact]
        public async Task when_try_scavenge_fails_returns_negative_one()
        {
            using (var fixture = await PostgresStreamStoreFixture.Create(testOutputHelper: TestOutputHelper))
            {
                var cts = new CancellationTokenSource();

                cts.Cancel();

                var result = await fixture.Store.TryScavenge(new StreamIdInfo("stream-1"), cts.Token);

                result.ShouldBe(-1);
            }
        }

        [Theory, InlineData("dbo"), InlineData("myschema")]
        public async Task Can_call_initialize_repeatably(string schema)
        {
            using(var fixture = await PostgresStreamStoreFixture.Create(schema, testOutputHelper: TestOutputHelper, createSchema: false))
            {
                await fixture.Store.CreateSchemaIfNotExists();
                await fixture.Store.CreateSchemaIfNotExists();
            }
        }

        [Fact]
        public async Task Can_drop_all()
        {
            var streamStoreObjects = new List<string>();

            string ReadInformationSchema((string name, string table) _)
                => $"SELECT {_.name}_name FROM information_schema.{_.table} WHERE {_.name}_schema = 'dbo'";

            using(var fixture = await PostgresStreamStoreFixture.Create(testOutputHelper: TestOutputHelper))
            {
                await fixture.Store.DropAll();

                var commandText = string.Join(
                    $"{Environment.NewLine}UNION{Environment.NewLine}",
                    new[]
                    {
                        ("table", "tables"),
                        ("sequence", "sequences"),
                        ("constraint", "table_constraints"),
                        ("user_defined_type", "user_defined_types"),
                        ("routine", "routines")
                    }.Select(ReadInformationSchema));

                using(var connection = new NpgsqlConnection(fixture.ConnectionString))
                {
                    await connection.OpenAsync().ConfigureAwait(false);

                    using(var command = new NpgsqlCommand(commandText, connection))
                    using(var reader = await command.ExecuteReaderAsync().ConfigureAwait(false))
                    {
                        while(await reader.ReadAsync().ConfigureAwait(false))
                        {
                            streamStoreObjects.Add(reader.GetString(0));
                        }
                    }
                }

                streamStoreObjects.ShouldBeEmpty();
            }
        }        
        
         
        [Fact]
        public void Can_export_database_creation_script()
        {
            string schema = "custom_schema";
            var store = new PostgresStreamStore(new PostgresStreamStoreSettings("server=.;database=sss")
            {
                Schema = schema,
            });

            var sqlScript = store.GetSchemaCreationScript();
            sqlScript.ShouldBe(new Scripts(schema).CreateSchema);
        }
        */

    }
}
