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

        protected async Task<(IStreamStore, Action, string)> GetStore(CancellationToken cancellationToken, string schema = "dbo")
        {
            IStreamStore streamStore = null;
            IDisposable disposable = null;
            string connectionString = null;

            Output.WriteLine(ConsoleColor.Yellow, "Store type:");
            await new Menu()
                .AddSync("InMem", () => streamStore = new InMemoryStreamStore())
                .Add("Postgres 9.6 (Docker)",
                    async ct =>
                    {
                        var fixture = new PostgresStreamStoreDb(schema, new Version(9, 6));
                        Console.WriteLine(fixture.ConnectionString);
                        
                        var gapHandlingInput = Input.ReadString("Use new gap handling (y/n): ");
                        var newGapHandlingEnabled = gapHandlingInput.ToLower() == "y";

                        await fixture.Start();
                        streamStore = await fixture.GetPostgresStreamStore(newGapHandlingEnabled ? new GapHandlingSettings(6000, 12000) : null);
                        disposable = fixture;
                        connectionString = fixture.ConnectionString;
                    })
                .Add("Postgres 14.5 (Docker)",
                    async ct =>
                    {
                        var fixture = new PostgresStreamStoreDb(schema, new Version(14, 5));
                        Console.WriteLine(fixture.ConnectionString);
                        
                        var gapHandlingInput = Input.ReadString("Use new gap handling (y/n): ");
                        var newGapHandlingEnabled = gapHandlingInput.ToLower() == "y";
                        
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
                        var postgresStreamStoreDb = new PostgresStreamStoreDb("dbo", new Version(9, 6), connectionString);
                        Console.WriteLine(postgresStreamStoreDb.ConnectionString);
                        
                        var gapHandlingInput = Input.ReadString("Use new gap handling (y/n): ");
                        var newGapHandlingEnabled = gapHandlingInput.ToLower() == "y";
                        
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
                },
                connectionString);
        }
    }
}