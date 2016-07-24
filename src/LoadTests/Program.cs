namespace LoadTests
{
    using System;
    using System.Collections.Generic;
    using System.Threading;
    using System.Threading.Tasks;
    using Serilog;
    using SqlStreamStore;
    using SqlStreamStore.Streams;

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
            PrintMenu();
            Console.ReadKey();
            cts.Cancel();
        }

        private static void PrintMenu()
        {
            Console.WriteLine("Load tests. Choose wisely.");
            Console.WriteLine($"(your processor count is {Environment.ProcessorCount}).\r\n");
            var foregroundColor = Console.ForegroundColor;
            Console.ForegroundColor = ConsoleColor.Yellow;
            Console.WriteLine(" 1. ExpectedVersion.Any on one stream.");
            Console.WriteLine(" 2. ExpectedVersion.Any on a stream per task.");
            Console.ForegroundColor = foregroundColor;
            Console.WriteLine();
        }

        private static async Task MainAsync(CancellationToken cancellationToken)
        {
            try
            {
                using(var fixture = new MsSqlStreamStoreFixture("dbo"))
                {
                    using(var store = await fixture.GetStreamStore())
                    {
                        using(var cts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken))
                        {
                            await RunLoadTest(cts, store);
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

        private static async Task RunLoadTest(CancellationTokenSource cts, IStreamStore streamStore)
        {
            var tasks = new List<Task>();
            int count = -1;

            for(int i = 0; i < Environment.ProcessorCount; i++)
            {
                var random = new Random();
                var task = Task.Run(async () =>
                {
                    while(!cts.IsCancellationRequested)
                    {
                        string info;
                        try
                        {
                            int streamNumber = random.Next(0, 100000);

                            var eventNumber = Interlocked.Increment(ref count);
                            var newStreamEvents = StreamStoreAcceptanceTests
                                .CreateNewStreamMessages(eventNumber*2 + 1, eventNumber*2 + 2);

                            info = $"{streamNumber} - {newStreamEvents[0].EventId}," +
                                       $"{newStreamEvents[1].EventId}";

                            Log.Logger.Information($"Begin {info}");
                            await streamStore.AppendToStream(
                                $"stream-{streamNumber}",
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