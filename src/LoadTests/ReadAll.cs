namespace LoadTests
{
    using System;
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.Threading;
    using System.Threading.Tasks;
    using EasyConsole;
    using Serilog;
    using SqlStreamStore;
    using SqlStreamStore.Streams;

    public class ReadAll : LoadTest
    {
        protected override async Task RunAsync(CancellationToken ct)
        {
            Output.WriteLine("");
            Output.WriteLine(ConsoleColor.Green, "Appends events to streams and reads them all back in a single task.");
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

            int parallelTasks = Input.ReadInt($"Number of parallel write tasks (Processor count = {Environment.ProcessorCount}): ", 1, 100);

            int numberOfMessagesToWrite = Input.ReadInt("Number message to append: ", 1, 10000000);

            int messageJsonDataSize = Input.ReadInt("Size of Json (kb): ", 1, 1024);

            int numberOfMessagesPerAmend = Input.ReadInt("Number of messages per amend: ", 1, 1000);

            int readPageSize = Input.ReadInt("Read page size: ", 1, 10000);

            var tasks = new List<Task>();
            int count = 0;
            string jsonData = new string('a', messageJsonDataSize * 1024);
            for (int i = 0; i < parallelTasks; i++)
            {
                var random = new Random();
                var task = Task.Run(async () =>
                {
                    var messageNumbers = new int[numberOfMessagesPerAmend];
                    while (!ct.IsCancellationRequested && count < numberOfMessagesToWrite)
                    {
                        try
                        {
                            int streamNumber = random.Next(0, numberOfStreams);

                            for (int j = 0; j < numberOfMessagesPerAmend; j++)
                            {
                                messageNumbers[j] = Interlocked.Increment(ref count);
                            }

                            var newmessages = StreamStoreAcceptanceTests
                                .CreateNewStreamMessages(jsonData, messageNumbers);

                            var info = $"{streamNumber} - {newmessages[0].MessageId}," +
                                       $"{newmessages[1].MessageId}";

                            Log.Logger.Information($"Begin {info}");
                            await streamStore.AppendToStream(
                                $"stream-{streamNumber}",
                                ExpectedVersion.Any,
                                newmessages,
                                ct);
                            Log.Logger.Information($"End   {info}");
                            Console.Write($"\r> {messageNumbers[numberOfMessagesPerAmend-1]}");
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
                page = await streamStore.ReadAllForwards(position, readPageSize, cancellationToken: ct);
                count += page.Messages.Length;
                Console.Write($"\r> Read {count}");
                position = page.NextPosition;
            } while (!page.IsEnd);

            stopwatch.Stop();
            var rate = Math.Round((decimal)count / stopwatch.ElapsedMilliseconds * 1000, 0);
            Output.WriteLine("");
            Output.WriteLine($"> {count} messages read in {stopwatch.Elapsed} ({rate} m/s)");
        }
    }
}