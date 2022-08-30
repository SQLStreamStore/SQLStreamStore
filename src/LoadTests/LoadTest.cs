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