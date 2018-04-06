namespace LoadTests
{
    using System;
    using System.Diagnostics;
    using System.Threading;
    using System.Threading.Tasks;
    using EasyConsole;
    using Serilog;
    using SqlStreamStore;
    using SqlStreamStore.Streams;

    public class UniqueStreams : LoadTest
    {
        protected override async Task RunAsync(CancellationToken ct)
        {
            Output.WriteLine("");
            Output.WriteLine(ConsoleColor.Green, "Appends events to streams(s) as quickly as possible.");
            Output.WriteLine(" - The stream ID is randomly choosen within a supplied range. The");
            Output.WriteLine("   larger the range the more chance of contention of write between");
            Output.WriteLine("   parallel tasks.");
            Output.WriteLine(" - The more parallel tasks, the more chance of contention of writes to a stream.");
            Output.WriteLine("");

            var (streamStore, dispose) = GetStore();

            try
            {
                await Append(streamStore, ct);
            }
            finally
            {
                dispose();
            }
        }

        public async Task<int> Append(IStreamStore streamStore, CancellationToken ct)
        {
            var numberOfStreams = Input.ReadInt("Number of streams: ", 1, 100000000);

            int messageJsonDataSize = Input.ReadInt("Size of Json (bytes): ", 1, 1024 * 1024);

            int numberOfMessagesPerAmend = Input.ReadInt("Number of messages per stream append: ", 1, 1000);

            string jsonData = new string('a', messageJsonDataSize);

            var stopwatch = Stopwatch.StartNew();
            var messageNumbers = new int[numberOfMessagesPerAmend];
            int count = 1;
            for (int i = 0; i < numberOfStreams; i++)
            {
                ct.ThrowIfCancellationRequested();
                try
                {
                    for (int j = 0; j < numberOfMessagesPerAmend; j++)
                    {
                        messageNumbers[j] = count++;
                    }

                    var newmessages = MessageFactory
                        .CreateNewStreamMessages(jsonData, messageNumbers);

                    await streamStore.AppendToStream(
                        $"stream-{i}",
                        ExpectedVersion.Any,
                        newmessages,
                        ct);
                    Console.Write($"\r> {messageNumbers[numberOfMessagesPerAmend - 1]}");
                }
                catch (Exception ex) when (!(ex is TaskCanceledException))
                {
                    Log.Logger.Error(ex, ex.Message);
                }
            }
            stopwatch.Stop();
            var rate = Math.Round((decimal)count / stopwatch.ElapsedMilliseconds * 1000, 0);

            Output.WriteLine("");
            Output.WriteLine($"> {count-1} messages written in {stopwatch.Elapsed} ({rate} m/s)");
            return count;
        }
    }
}