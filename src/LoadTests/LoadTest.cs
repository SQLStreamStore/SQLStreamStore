namespace LoadTests
{
    using System;
    using System.Threading;
    using System.Threading.Tasks;
    using EasyConsole;
    using SqlStreamStore;

    public abstract class LoadTest
    {
        public void Run(CancellationToken cancellationToken)
        {
            Task.Run(() => RunAsync(cancellationToken)).GetAwaiter().GetResult();
        }

        protected abstract Task RunAsync(CancellationToken cancellationToken);

        protected (IStreamStore, Action) GetStore()
        {
            IStreamStore streamStore = null;
            IDisposable disposable = null;

            Output.WriteLine(ConsoleColor.Yellow, "Store type:");
            new Menu()
                .Add("InMem", () => streamStore = new InMemoryStreamStore())
                .Add("MS SQL (Docker)",
                    () =>
                    {
                        var fixture = new MsSqlStreamStoreV3Fixture("dbo");
                        Console.WriteLine(fixture.ConnectionString);
                        streamStore = fixture.GetStreamStore().Result;
                        disposable = fixture;
                    })
                .Add("Postgres (Docker)",
                    () =>
                    {
                        var fixture = new PostgresStreamStoreFixture("dbo");
                        Console.WriteLine(fixture.ConnectionString);
                        streamStore = fixture.GetPostgresStreamStore(true).Result;
                        disposable = fixture;
                    })
                .Add("Postgres (Server)",
                    () =>
                    {
                        Console.Write("Enter the connection string: ");
                        var connectionString = Console.ReadLine();
                        var fixture = new PostgresStreamStoreFixture("dbo", connectionString);
                        Console.WriteLine(fixture.ConnectionString);
                        streamStore = fixture.GetPostgresStreamStore(true).Result;
                        disposable = fixture;
                    })
                .Display();

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