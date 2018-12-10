namespace SqlStreamStore
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Threading;
    using System.Threading.Tasks;
    using Npgsql;
    using Shouldly;
    using SqlStreamStore.Infrastructure;
    using SqlStreamStore.PgSqlScripts;
    using SqlStreamStore.Streams;
    using Xunit;
    using Xunit.Abstractions;

    public class PostgresStreamStoreAcceptanceTests : StreamStoreAcceptanceTests
    {
        public PostgresStreamStoreAcceptanceTests(ITestOutputHelper testOutputHelper)
            : base(testOutputHelper)
        { }

        protected override StreamStoreAcceptanceTestFixture GetFixture()
            => new PostgresStreamStoreFixture("foo", TestOutputHelper);

        [Fact]
        public async Task Can_use_multiple_schemas()
        {
            using(var fixture = new PostgresStreamStoreFixture("dbo", TestOutputHelper))
            {
                using(var dboStore = await fixture.GetStreamStore())
                {
                    using(var barStore = await fixture.GetStreamStore("bar"))
                    {
                        await dboStore.AppendToStream("stream-1",
                            ExpectedVersion.NoStream,
                            CreateNewStreamMessages(1, 2));
                        await barStore.AppendToStream("stream-1",
                            ExpectedVersion.NoStream,
                            CreateNewStreamMessages(1, 2));

                        var dboHeadPosition = await dboStore.ReadHeadPosition();
                        var fooHeadPosition = await dboStore.ReadHeadPosition();

                        dboHeadPosition.ShouldBe(1);
                        fooHeadPosition.ShouldBe(1);
                    }
                }
            }
        }

        [Fact]
        public async Task when_try_scavenge_fails_returns_negative_one()
        {
            using(var fixture = new PostgresStreamStoreFixture("dbo", TestOutputHelper))
            {
                using(var store = await fixture.GetPostgresStreamStore())
                {
                    var cts = new CancellationTokenSource();

                    cts.Cancel();

                    var result = await store.TryScavenge(new StreamIdInfo("stream-1"), cts.Token);

                    result.ShouldBe(-1);
                }
            }
        }

        [Theory, InlineData("dbo"), InlineData("myschema")]
        public async Task Can_call_initialize_repeatably(string schema)
        {
            using(var fixture = new PostgresStreamStoreFixture(schema, TestOutputHelper))
            {
                using(var store = await fixture.GetUninitializedPostgresStreamStore())
                {
                    await store.CreateSchema();
                    await store.CreateSchema();
                }
            }
        }

        [Fact]
        public async Task Can_drop_all()
        {
            var streamStoreObjects = new List<string>();

            string ReadInformationSchema((string name, string table) _)
                => $"SELECT {_.name}_name FROM information_schema.{_.table} WHERE {_.name}_schema = 'dbo'";

            using(var fixture = new PostgresStreamStoreFixture("dbo", TestOutputHelper))
            {
                using(var store = await fixture.GetPostgresStreamStore())
                {
                    await store.DropAll();
                }

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
                    await connection.OpenAsync().NotOnCapturedContext();

                    using(var command = new NpgsqlCommand(commandText, connection))
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
            using(var fixture = new PostgresStreamStoreFixture("dbo", TestOutputHelper))
            {
                using(var store = await fixture.GetPostgresStreamStore())
                {
                    var result = await store.CheckSchema();

                    result.ShouldBe(new CheckSchemaResult(1, 1));
                    result.IsMatch.ShouldBeTrue();
                }
            }
        }

        public static IEnumerable<object[]> GetUtcNowNullCases()
        {
            var message = CreateNewStreamMessages(1).First();
            yield return new object[]
            {
                new Func<PostgresStreamStore, Task>(
                    store => store.AppendToStream("a-stream", ExpectedVersion.Any, message)),
            };
            yield return new object[]
            {
                new Func<PostgresStreamStore, Task>(
                    async store =>
                    {
                        await store.AppendToStream("a-stream", ExpectedVersion.Any, message);
                        await store.DeleteStream("a-stream");
                    })
            };
            yield return new object[]
            {
                new Func<PostgresStreamStore, Task>(
                    async store =>
                    {
                        await store.AppendToStream("a-stream", ExpectedVersion.Any, message);
                        await store.DeleteMessage("a-stream", message.MessageId);
                    }),
            };
            yield return new object[]
            {
                new Func<PostgresStreamStore, Task>(
                    store => store.SetStreamMetadata("a-stream", maxAge: 1))
            };
        }

        [Theory, MemberData(nameof(GetUtcNowNullCases))]
        public async Task Can_invoke_operation_when_get_utc_now_is_null(Func<PostgresStreamStore, Task> operation)
        {
            using(var fixture = new PostgresStreamStoreFixture("dbo", TestOutputHelper))
            {
                await fixture.CreateDatabase();

                using(var store = new PostgresStreamStore(new PostgresStreamStoreSettings(fixture.ConnectionString)
                {
                    GetUtcNow = null
                }))
                {
                    await store.CreateSchema();

                    await operation(store);
                }
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
    }
}