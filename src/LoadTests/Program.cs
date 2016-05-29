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
                //.WriteTo
                //.ColoredConsole()
                .WriteTo
                .File("LoadTests.txt")
                .CreateLogger();

            var cts = new CancellationTokenSource();
            var task = MainAsync(cts.Token);
            Console.WriteLine("Press key to stop...");
            Console.ReadKey();
            cts.Cancel();
        }

        private static async Task MainAsync(CancellationToken cancellationToken)
        {
            try
            {
                using(var fixture = new MsSqlEventStoreFixture("dbo"))
                {
                    using(var eventStore = await fixture.GetEventStore())
                    {
                        using(var cts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken))
                        {
                            await RunLoadTest(cts, eventStore);
                        }
                    }
                }
            }
            catch(Exception ex)
            {
                Console.WriteLine(ex.Message);
                throw;
            }
        }

        private static async Task RunLoadTest(CancellationTokenSource cts, IEventStore eventStore)
        {
            var tasks = new List<Task>();
            int count = -1;

            for(int i = 0; i < 2; i++)
            {
                var taskNumber = i;
                var task = Task.Run(async () =>
                {
                    while(!cts.IsCancellationRequested)
                    {
                        try
                        {
                            var eventNumber = Interlocked.Increment(ref count);
                            var newStreamEvents = EventStoreAcceptanceTests
                                .CreateNewStreamEvents(eventNumber*2 + 1, eventNumber*2 + 2);

                            var info = $"{taskNumber} - {newStreamEvents[0].EventId}," +
                                       $"{newStreamEvents[1].EventId}";

                            Log.Logger.Information($"Begin {info}");
                            await eventStore.AppendToStream(
                                "stream-1",
                                ExpectedVersion.Any,
                                newStreamEvents,
                                cts.Token);
                            Log.Logger.Information($"End   {info}");
                            Console.Write($"\r{eventNumber*2 + 2}");
                        }
                        catch(Exception ex) when(!(ex is TaskCanceledException))
                        {
                            cts.Cancel();
                            Log.Logger.Error(ex, ex.Message);
                            Console.WriteLine(ex);
                            Console.ReadKey();
                        }
                    }
                },cts.Token);
                tasks.Add(task);
            }
            await Task.WhenAll(tasks);
        }
    }
}