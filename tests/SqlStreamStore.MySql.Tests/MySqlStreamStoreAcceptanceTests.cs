namespace SqlStreamStore
{
    using System.Threading.Tasks;
    using Xunit;
    using Xunit.Abstractions;

    public class MySqlStreamStoreAcceptanceTests : AcceptanceTests, IClassFixture<MySqlStreamStoreFixturePool>
    {
        private readonly MySqlStreamStoreFixturePool _fixturePool;

        public MySqlStreamStoreAcceptanceTests(
            MySqlStreamStoreFixturePool fixturePool,
            ITestOutputHelper testOutputHelper)
            : base(testOutputHelper)
        {
            _fixturePool = fixturePool;
        }

        protected override async Task<IStreamStoreFixture> CreateFixture()
            => await _fixturePool.Get(TestOutputHelper);

        /*
        [Fact]
        public async Task Can_use_multiple_databases()
        {
            using(var dboFixture = await CreateFixture())
            using(var barFixture = await CreateFixture())
            {
                await dboFixture.Store.AppendToStream("stream-1",
                    ExpectedVersion.NoStream,
                    CreateNewStreamMessages(1, 2));
                await barFixture.Store.AppendToStream("stream-1",
                    ExpectedVersion.NoStream,
                    CreateNewStreamMessages(1, 2));

                var dboHeadPosition = await dboFixture.Store.ReadHeadPosition();
                var barHeadPosition = await barFixture.Store.ReadHeadPosition();

                dboHeadPosition.ShouldBe(1);
                barHeadPosition.ShouldBe(1);
            }
        }

        [Fact]
        public async Task when_try_scavenge_fails_returns_negative_one()
        {
            using (var fixture = await MySqlStreamStoreFixture.Create(TestOutputHelper))
            {
                var cts = new CancellationTokenSource();

                cts.Cancel();

                var result = await fixture.Store.TryScavenge(new StreamIdInfo("stream-1"), cts.Token);

                result.ShouldBe(-1);
            }
        }
        [Fact]
        public async Task Can_call_initialize_repeatably()
        {
            using(var fixture = await MySqlStreamStoreFixture.Create(TestOutputHelper, false))
            {
                await fixture.Store.CreateSchemaIfNotExists();
                await fixture.Store.CreateSchemaIfNotExists();
            }
        }

        [Fact]
        public async Task Can_drop_all()
        {
            var streamStoreObjects = new List<string>();

            using(var fixture = await MySqlStreamStoreFixture.Create(TestOutputHelper))
            {
                string ReadInformationSchema((string name, string table) _)
                    => $"SELECT {_.name}_name FROM information_schema.{_.table} WHERE {_.name}_schema = '{fixture.DatabaseName}'";

                await fixture.Store.DropAll();

                var commandText = string.Join(
                    $"{Environment.NewLine}UNION{Environment.NewLine}",
                    new[]
                    {
                        ("table", "tables"),
                        ("constraint", "table_constraints"),
                        ("routine", "routines")
                    }.Select(ReadInformationSchema));

                using(var connection = new MySqlConnection(fixture.ConnectionString))
                {
                    await connection.OpenAsync().NotOnCapturedContext();

                    using(var command = new MySqlCommand(commandText, connection))
                    using(var reader = await command.ExecuteReaderAsync().NotOnCapturedContext())
                    {
                        while(await reader.ReadAsync().NotOnCapturedContext())
                        {
                            streamStoreObjects.Add(reader.GetString(0));
                        }
                    }
                }

                streamStoreObjects.ShouldBeEmpty();
            }
        }

        [Fact]
        public async Task Can_check_schema()
        {
            using(var fixture = await MySqlStreamStoreFixture.Create(TestOutputHelper))
            {
                var result = await fixture.Store.CheckSchema();

                result.ShouldBe(new CheckSchemaResult(1, 1));
                result.IsMatch.ShouldBeTrue();
            }
        }

        [Fact]
        public void Can_export_database_creation_script()
        {
            var store = new MySqlStreamStore(new MySqlStreamStoreSettings("server=.;database=sss"));

            var sqlScript = store.GetSchemaCreationScript();
            sqlScript.ShouldBe(new Scripts().CreateSchema);
        }
        */
    }
}