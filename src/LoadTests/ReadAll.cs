namespace LoadTests
{
    using System;
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.Text;
    using System.Threading;
    using System.Threading.Tasks;
    using EasyConsole;
    using Serilog;
    using SqlStreamStore;
    using SqlStreamStore.Streams;

    public class ReadAll : PerformanceTest
    {
        protected override async Task RunAsync(CancellationToken ct)
        {
            Output.WriteLine("");
            Output.WriteLine(ConsoleColor.Green, "Appends events to streams and reads them all back");
            Output.WriteLine("");

            var streamStore = GetStore();

            int numberOfStreams = -1;
            Output.WriteLine(ConsoleColor.Yellow, "Number of streams:");
            new Menu()
                .Add("1", () => numberOfStreams = 1)
                .Add("10", () => numberOfStreams = 10)
                .Add("100", () => numberOfStreams = 100)
                .Add("1000", () => numberOfStreams = 1000)
                .Add("10000", () => numberOfStreams = 10000)
                .Add("Custom", () => { })
                .Display();

            int parallelTasks = Input.ReadInt(
                $"Number of parallel tasks (Processor count = {Environment.ProcessorCount}): ", 1, 100);

            int numberOfMessagesToWrite = Input.ReadInt(
                $"Number message to append: ", 1, 10000000);

            var tasks = new List<Task>();
            int count = 0;
            for (int i = 0; i < parallelTasks; i++)
            {
                var random = new Random();
                var task = Task.Run(async () =>
                {
                    while (!ct.IsCancellationRequested && count < numberOfMessagesToWrite)
                    {
                        try
                        {
                            int streamNumber = random.Next(0, numberOfStreams);

                            var messageNumber = Interlocked.Increment(ref count);
                            var messageNumber2 = Interlocked.Increment(ref count);
                            var newmessages = StreamStoreAcceptanceTests
                                .CreateNewStreamMessages(messageNumber, messageNumber2);

                            var length = Encoding.UTF8.GetBytes(newmessages[0].JsonData).Length;

                            var info = $"{streamNumber} - {newmessages[0].MessageId}," +
                                       $"{newmessages[1].MessageId}";

                            Log.Logger.Information($"Begin {info}");
                            await streamStore.AppendToStream(
                                $"stream-{streamNumber}",
                                ExpectedVersion.Any,
                                newmessages,
                                ct);
                            Log.Logger.Information($"End   {info}");
                            Console.Write($"\r{messageNumber2}");
                        }
                        catch (Exception ex) when (!(ex is TaskCanceledException))
                        {
                            Log.Logger.Error(ex, ex.Message);
                        }
                    }
                }, ct);
                tasks.Add(task);
            }
            await Task.WhenAll(tasks);

            Console.WriteLine("");
            var stopwatch = Stopwatch.StartNew();
            count = 0;
            var position = Position.Start;
            ReadAllPage page;
            do
            {
                page = await streamStore.ReadAllForwards(position, 100, cancellationToken: ct);
                count += page.Messages.Length;
                Console.Write($"\rRead {count}");
                position = page.NextPosition;
            } while (!page.IsEnd);

            stopwatch.Stop();
            var rate = Math.Round((decimal)count / stopwatch.ElapsedMilliseconds * 1000, 0);
            Output.WriteLine("");
            Output.WriteLine($"{count} messages read in {stopwatch.Elapsed} ({rate} m/s)");
        }
    }
}