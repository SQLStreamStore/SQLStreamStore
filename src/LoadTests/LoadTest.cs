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

        protected async Task<(IStreamStore, Action)> GetStore(CancellationToken cancellationToken, string schema = "public")
        {
            IStreamStore streamStore = null;
            IDisposable disposable = null;
            string connectionString = null;

            Output.WriteLine(ConsoleColor.Yellow, "Store type:");
            await new Menu()
                .AddSync("InMem", () => streamStore = new InMemoryStreamStore())
                .Add("Postgres (Docker)",
                    async ct =>
                    {
                        var fixture = new PostgresStreamStoreDb(schema);
                        Console.WriteLine(fixture.ConnectionString);
                        
                        var newGapHandlingEnabled = await Input.ReadEnum<YesNo>("Use new gap handling: ", ct) == YesNo.Yes;
                        
                        await fixture.Start();
                        streamStore = await fixture.GetPostgresStreamStore(newGapHandlingEnabled ? new GapHandlingSettings(6000, 12000) : null);
                        disposable = fixture;
                        connectionString = fixture.ConnectionString;
                    })
                .Add("Postgres (Server)",
                    async ct =>
                    {
                        Console.Write("Enter the connection string: ");
                        connectionString = Console.ReadLine();
                        var postgresStreamStoreDb = new PostgresStreamStoreDb("dbo", connectionString);
                        Console.WriteLine(postgresStreamStoreDb.ConnectionString);
                        
                        var newGapHandlingEnabled = await Input.ReadEnum<YesNo>("Use new gap handling: ", ct) == YesNo.Yes;
                        
                        streamStore = await postgresStreamStoreDb.GetPostgresStreamStore(newGapHandlingEnabled ? new GapHandlingSettings(6000, 12000) : null);
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
        
        internal enum YesNo
        {
            Yes,
            No
        }
    }
}