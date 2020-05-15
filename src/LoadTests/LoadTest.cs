namespace LoadTests
{
    using System;
    using System.Threading;
    using System.Threading.Tasks;
    using EasyConsole;
    using SqlStreamStore;

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
                .Add("MYSQL V3 (Docker)",
                    async ct =>
                    {
                        var mysqlDb = new MySqlStreamStoreDb();
                        streamStore = await mysqlDb.GetMySqlStreamStore();
                        disposable = streamStore;
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
                        var postgresStreamStoreDb = new PostgresStreamStoreDb("dbo", connectionString);
                        Console.WriteLine(postgresStreamStoreDb.ConnectionString);
                        streamStore = await postgresStreamStoreDb.GetPostgresStreamStore(true);
                        disposable = postgresStreamStoreDb;
                    })
                .Add("MySql (Docker)",
                    async ct =>
                    {
                        var db = new MySqlStreamStoreDb();
                        streamStore = await db.GetStreamStore();
                        disposable = db;
                    })
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