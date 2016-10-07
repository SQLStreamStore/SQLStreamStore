namespace LoadTests
{
    using System;
    using System.Threading;
    using System.Threading.Tasks;
    using Serilog;
    using SqlStreamStore;

    internal class Program
    {
        static void Main(string[] args)
        {
            Log.Logger = new LoggerConfiguration()
                .WriteTo
                .File("LoadTests.txt")
                .CreateLogger();

            var cts = new CancellationTokenSource();

            Func<Task<IStreamStore>> _createInmemoryStore = async () =>
            {
                var store = new InMemoryStreamStore();
                await store.InitializeStore();
                return store;
            };

            Func<Task<IStreamStore>> _createMsSqlStore = async () =>
            {
                var fixture = new MsSqlStreamStoreFixture("dbo");
                var store = await fixture.GetStreamStore();
                return store;
            };

            var test = new AppendExpectedVersionAnySingleThread(_createInmemoryStore);
            var task = Task.Run(() => test.Run(cts.Token));
            Console.ReadLine();
            cts.Cancel();
            task.GetAwaiter().GetResult();
        }
    }
}