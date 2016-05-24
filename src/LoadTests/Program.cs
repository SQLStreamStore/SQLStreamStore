namespace LoadTests
{
    using System;
    using System.Collections.Generic;
    using System.Threading;
    using System.Threading.Tasks;
    using Cedar.EventStore;
    using Cedar.EventStore.Streams;
    using Serilog;

    internal class Program
    {
        static void Main(string[] args)
        {
            Log.Logger = new LoggerConfiguration()
                .WriteTo
                .ColoredConsole()
                .CreateLogger();

            var cts = new CancellationTokenSource();
            var task = MainAsync(cts.Token);
            Console.WriteLine("Press key to stop...");
            Console.ReadKey();
            cts.Cancel();
        }

        private static async Task MainAsync(CancellationToken cancellationToken)
        {
            using(var fixture = new MsSqlEventStoreFixture("dbo"))
            {
                using(var eventStore = await fixture.GetEventStore())
                {
                    using(var cts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken))
                    {
                        var tasks = new List<Task>();
                        int count = 0;

                        for(int i = 0; i < Environment.ProcessorCount; i++)
                        {
                            var task = Task.Run(async () =>
                            {
                                while(!cts.IsCancellationRequested)
                                {
                                    try
                                    {
                                        await eventStore.AppendToStream("stream-1",
                                            ExpectedVersion.Any,
                                            EventStoreAcceptanceTests.CreateNewStreamEvents(1, 2),
                                            cts.Token);
                                        Console.Write($"\r{Interlocked.Increment(ref count)}");
                                    }
                                    catch (Exception ex) when(!(ex is TaskCanceledException))
                                    {
                                        cts.Cancel();
                                        Console.WriteLine(ex);
                                    }
                                }
                            },cts.Token);
                            tasks.Add(task);
                        }
                        await Task.WhenAll(tasks);
                    }
                }
            }
        }
    }
}