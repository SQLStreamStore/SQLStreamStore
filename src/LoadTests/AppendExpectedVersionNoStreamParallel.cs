namespace LoadTests
{
    using System;
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.Linq;
    using System.Threading;
    using System.Threading.Tasks;
    using EasyConsole;
    using Serilog;
    using SqlStreamStore;
    using SqlStreamStore.Streams;

    public class AppendExpectedVersionNoStreamParallel : LoadTest
    {
        public override async Task Run(CancellationToken ct)
        {
            Output.WriteLine("");
            Output.WriteLine(ConsoleColor.Green, "Appends events to streams(s) as quickly as possible.");
            Output.WriteLine(" - The stream ID is randomly chosen within a supplied range. The");
            Output.WriteLine("   smaller the range the more chance of contention of writes between");
            Output.WriteLine("   parallel tasks.");
            Output.WriteLine(" - The more parallel tasks, the more chance of contention of writes to a stream.");
            Output.WriteLine("");

            var (streamStore, dispose, _) = await GetStore(ct);

            try
            {
                await Append(streamStore, ct);
            }
            finally
            {
                dispose();
            }
        }

        private async Task<int> Append(IStreamStore streamStore, CancellationToken ct)
        {
            var numberOfStreams = 100;
            int parallelTasks = 16;
            //var numberOfStreams = Input.ReadInt("Number of streams: ", 1, 100000000);

            //int parallelTasks = Input.ReadInt(
            //    $"Number of parallel write tasks (Processor count = {Environment.ProcessorCount}): ",
            //    1,
            //    100);

            //int numberOfMessagesToWrite = Input.ReadInt(
            //    $"Number message to append: ",
            //    1,
            //    10000000);

            int messageJsonDataSize = 1024;
            //int messageJsonDataSize = Input.ReadInt("Size of Json (bytes): ", 1, 10 * 1024 * 1024);

            int numberOfMessagesPerAmend = 2;
            //int numberOfMessagesPerAmend = Input.ReadInt("Number of messages per append: ", 1, 1000);

            var tasks = new List<Task>();
            int count = 0;

            Queue<int> streamsToInsert = new Queue<int>(Enumerable.Range(0, numberOfStreams));

            string jsonData = $@"{{""b"": ""{new string('a', messageJsonDataSize * 1024)}""}}";
            var stopwatch = Stopwatch.StartNew();
            for(int i = 0; i < parallelTasks; i++)
            {
                var random = new Random();
                var task = Task.Run(async () =>
                    {
                        while(!ct.IsCancellationRequested && streamsToInsert.TryDequeue(out var streamNumber))
                        {
                            var messageNumbers = new int[numberOfMessagesPerAmend];
                            try
                            {

                                for(int j = 0; j < numberOfMessagesPerAmend; j++)
                                {
                                    messageNumbers[j] = Interlocked.Increment(ref count);
                                }

                                var newMessages = MessageFactory
                                    .CreateNewStreamMessages(jsonData, messageNumbers);

                                var info = $"{streamNumber}";

                                Log.Logger.Information($"Begin {info}");
                                await streamStore.AppendToStream(
                                    $"stream-{streamNumber}",
                                    ExpectedVersion.NoStream,
                                    newMessages,
                                    ct);
                                Log.Logger.Information($"End   {info}");
                                Console.Write($"\r> {messageNumbers[numberOfMessagesPerAmend - 1]}");
                            }
                            catch(Exception ex) when(!(ex is TaskCanceledException))
                            {
                                Log.Logger.Error(ex, ex.Message);
                                Output.WriteLine(ex.ToString());
                            }
                        }
                    },
                    ct);
                tasks.Add(task);
            }

            await Task.WhenAll(tasks);
            stopwatch.Stop();
            var rate = Math.Round((decimal) count / stopwatch.ElapsedMilliseconds * 1000, 0);

            Output.WriteLine("");
            Output.WriteLine($"> {count} messages written in {stopwatch.Elapsed} ({rate} m/s)");
            return count;
        }
    }
}