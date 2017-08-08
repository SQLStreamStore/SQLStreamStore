﻿namespace LoadTests
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

    public class AppendExpectedVersionAnyParallel : LoadTest
    {
        protected override async Task RunAsync(CancellationToken ct)
        {
            Output.WriteLine("");
            Output.WriteLine(ConsoleColor.Green, "Appends events to streams(s) as quickly as possible.");
            Output.WriteLine(" - The stream ID is randomly choosen within a supplied range. The");
            Output.WriteLine("   larger the range the more chance of contention of writes between");
            Output.WriteLine("   parallel tasks.");
            Output.WriteLine(" - The more parallel tasks, the more chance of contention of writes to a stream.");
            Output.WriteLine("");

            var streamStore = GetStore();

            await Append(streamStore, ct);
        }

        public async Task<int> Append(IStreamStore streamStore, CancellationToken ct)
        {
            var numberOfStreams = Input.ReadInt("Number of streams: ", 1, 100000000);

            int parallelTasks = Input.ReadInt(
                $"Number of parallel write tasks (Processor count = {Environment.ProcessorCount}): ", 1, 100);

            int numberOfMessagesToWrite = Input.ReadInt(
                $"Number message to append: ", 1, 10000000);

            int messageJsonDataSize = Input.ReadInt("Size of Json (kb): ", 1, 1024);

            int numberOfMessagesPerAmend = Input.ReadInt("Number of messages per append: ", 1, 1000);

            var tasks = new List<Task>();
            int count = 0;
            string jsonData = new string('a', messageJsonDataSize * 1024);
            var stopwatch = Stopwatch.StartNew();
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
                            Console.Write($"\r> {messageNumbers[numberOfMessagesPerAmend - 1]}");
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
            stopwatch.Stop();
            var rate = Math.Round((decimal)count / stopwatch.ElapsedMilliseconds * 1000, 0);

            Output.WriteLine("");
            Output.WriteLine($"> {count} messages written in {stopwatch.Elapsed} ({rate} m/s)");
            return count;
        }
    }
}
