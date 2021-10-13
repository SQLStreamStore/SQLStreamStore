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