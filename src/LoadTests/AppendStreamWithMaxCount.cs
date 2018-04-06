namespace LoadTests
{
    using System;
    using System.Diagnostics;
    using System.Threading;
    using System.Threading.Tasks;
    using EasyConsole;
    using SqlStreamStore;
    using SqlStreamStore.Streams;

    public class AppendMaxCount : LoadTest
    {
        protected override async Task RunAsync(CancellationToken ct)
        {
            Output.WriteLine("");
            Output.WriteLine(ConsoleColor.Green, "Appends events to a single stream that has a maxCount.");
            Output.WriteLine("");

            var (streamStore, dispose) = GetStore();

            try
            {

                int numberOfMessagesToWrite = Input.ReadInt(
                    "Number message to append: ",
                    1,
                    1000000);

                int messageJsonDataSize = Input.ReadInt("Size of Json (bytes): ", 1, 1024 * 1024);

                int maxCount = Input.ReadInt("Metadata maxCount: ", 0, 1000);

                int numberOfMessagesPerAmend = Input.ReadInt("Number of messages per amend: ", 1, 1000);

                int count = 1;
                const string streamId = "stream";
                await streamStore.SetStreamMetadata(streamId,
                    ExpectedVersion.NoStream,
                    maxCount: maxCount,
                    cancellationToken: ct);

                var messageNumbers = new int[numberOfMessagesPerAmend];
                string jsonData = new string('a', messageJsonDataSize * 1024);

                var stopwatch = Stopwatch.StartNew();
                while(count < numberOfMessagesToWrite)
                {
                    for(int j = 0; j < numberOfMessagesPerAmend; j++)
                    {
                        messageNumbers[j] = count++;
                    }

                    await streamStore.AppendToStream(streamId,
                        ExpectedVersion.Any,
                        MessageFactory.CreateNewStreamMessages(jsonData, messageNumbers),
                        cancellationToken: ct);

                    Console.Write($"\r> {messageNumbers[numberOfMessagesPerAmend - 1]}");
                }

                stopwatch.Stop();
                var rate = Math.Round((decimal) count / stopwatch.ElapsedMilliseconds * 1000, 0);

                Output.WriteLine("");
                Output.WriteLine($"> {count} messages written in {stopwatch.Elapsed} ({rate} m/s)");

                var streampage = await streamStore.ReadStreamForwards(streamId, StreamVersion.Start, maxCount + 1, ct);

                Output.WriteLine($"> Stream Message length: {streampage.Messages.Length}");
            }
            finally
            {
                dispose();
            }
        }
    }
}