namespace LoadTests
{
    using System;
    using System.Threading;
    using System.Threading.Tasks;
    using EasyConsole;
    using SqlStreamStore;
    using SqlStreamStore.Oracle.Tests;

    public abstract class LoadTest
    {
        public abstract Task Run(CancellationToken cancellationToken);

        protected async Task<(IStreamStore, Action)> GetStore(CancellationToken cancellationToken)
        {
            IStreamStore streamStore = null;
            IDisposable disposable = null;

            Output.WriteLine(ConsoleColor.Yellow, "Store type:");
            await new Menu()
                .AddSync("InMem", () => streamStore = new InMemoryStreamStore())
                .Add("MS SQL V2 (Docker)",
                    async _ =>
                    {
                        var fixture = new MsSqlStreamStoreDb("dbo");
                        Console.WriteLine(fixture.ConnectionString);
                        streamStore = await fixture.GetStreamStore();
                        disposable = fixture;
                    })
                .Add("MS SQL V3 (Docker)",
                    async _ =>
                    {
                        var fixture = new MsSqlStreamStoreDbV3("dbo");
                        Console.WriteLine(fixture.ConnectionString);
                        streamStore = await fixture.GetStreamStore();
                        disposable = fixture;
                    })
                .AddSync("MS SQL V3 (LocalDB)",
                    () =>
                    {
                        var sqlLocalDb = new SqlLocalDb();
                        Console.WriteLine(sqlLocalDb.ConnectionString);
                        streamStore = sqlLocalDb.StreamStore;
                        disposable = sqlLocalDb;
                    })
                .Add("Postgres (Docker)",
                    async ct =>
                    {
                        var fixture = new PostgresStreamStoreDb("dbo");
                        Console.WriteLine(fixture.ConnectionString);
                        streamStore = await fixture.GetPostgresStreamStore(true);
                        disposable = fixture;
                    })
                .Add("Postgres (Server)",
                    async ct =>
                    {
                        Console.Write("Enter the connection string: ");
                        var connectionString = Console.ReadLine();
                        var fixture = new PostgresStreamStoreDb("dbo", connectionString);
                        Console.WriteLine(fixture.ConnectionString);
                        streamStore = await fixture.GetPostgresStreamStore(true);
                        disposable = fixture;
                    })
                .Add("Oracle (Server)",
                    async ct =>
                    {
                        var connString = "user id=REL;password=REL;data source=localhost:41521/ORCLCDB;" +
                                         "Min Pool Size=50;Connection Lifetime=120;Connection Timeout=60;" +
                                         "Incr Pool Size=5; Decr Pool Size=2";

                        var fixture = await OracleFixture.CreateFixture(connString);
                        
                        streamStore = fixture.Store;
                        disposable = fixture;
                    })
                /*.Add("MySql (Docker)",
                    async ct =>
                    {
                        var fixture = new MySqlStreamStoreDb();
                        Console.WriteLine(fixture.ConnectionString);
                        streamStore = await fixture.GetMySqlStreamStore(true);
                        disposable = fixture;
                    })*/
                /*.Add("MySql (Server)",
                    async ct =>
                    {
                        Console.Write("Enter the connection string: ");
                        var connectionString = Console.ReadLine();
                        var fixture = new MySqlStreamStoreDb(connectionString);
                        Console.WriteLine(fixture.ConnectionString);
                        streamStore = await fixture.GetMySqlStreamStore(true);
                        disposable = fixture;
                    })*/
                .Display(cancellationToken);

            return (
                streamStore,
                () =>
                {
                    streamStore.Dispose();
                    disposable?.Dispose();
                });
        }
    }
}