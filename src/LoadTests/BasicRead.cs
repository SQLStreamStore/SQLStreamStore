using EasyConsole;
using SqlStreamStore.Streams;
using System.Collections.Generic;
using System.Diagnostics;
using System.Threading.Tasks;
using System.Threading;
using System;

namespace LoadTests
{
internal class BasicRead : LoadTest
    {
        public override async Task Run(CancellationToken ct)
        {
            Output.WriteLine("");
            Output.WriteLine(ConsoleColor.Green, "Appends events to a single stream and reads all");
            Output.WriteLine("");

            var (streamStore, dispose) = await GetStore(ct);

            try
            {
                var numberOfStreams = Input.ReadInt("Number of streams: ", 1, 100000000);
                int messageJsonDataSize = Input.ReadInt("Size of Json (kb): ", 1, 1024);
                int numberOfMessagesPerAmend = Input.ReadInt("Number of messages per stream append: ", 1, 1000);

                int readPageSize = Input.ReadInt("Read page size: ", 1, 10000);

                var messageNumbers = new int[numberOfMessagesPerAmend];
                string jsonData = $@"{{""b"": ""{new string('a', messageJsonDataSize * 1024)}""}}";

                var stopwatch = Stopwatch.StartNew();
                int count = 1;

                var l = new List<Task>();
                for (int i = 0; i < 10; i++)
                {
                    var task = Task.Run(async () =>
                    {
                        for (int j = 0; j < numberOfStreams; j++)
                        {
                            ct.ThrowIfCancellationRequested();
                            try
                            {
                                for (int t = 0; t < numberOfMessagesPerAmend; t++)
                                {
                                    messageNumbers[t] = count++;
                                }

                                var newMessages = MessageFactory
                                    .CreateNewStreamMessages(jsonData, messageNumbers);

                                await streamStore.AppendToStream(
                                    $"stream-{j + i}",
                                    ExpectedVersion.Any,
                                    newMessages,
                                    ct);
                                // Uncomment if you want more logging in console
                                // Console.Write($"> {messageNumbers[numberOfMessagesPerAmend - 1]}");
                            }
                            catch (Exception ex) when (!(ex is TaskCanceledException))
                            {
                                Output.WriteLine(ex.ToString());
                            }
                        }
                    });
                    l.Add(task);
                }

                await Task.WhenAll(l);

                stopwatch.Stop();
                var rate = Math.Round((decimal)count / stopwatch.ElapsedMilliseconds * 1000, 0);

                Output.WriteLine("");
                Output.WriteLine($"> {count - 1} messages written in {stopwatch.Elapsed} ({rate} m/s)");

                Output.WriteLine("");
                Output.WriteLine($"> {count} messages written in {stopwatch.Elapsed} ({rate} m/s)");

                ReadAllPage readAllPage = await streamStore.ReadAllForwards(StreamVersion.Start, readPageSize, true, ct);
                do
                {
                    readAllPage = await readAllPage.ReadNext(ct);
                } while (!readAllPage.IsEnd);


                Output.WriteLine($"> Stream Message length: {readAllPage.Messages.Length}");
            }
            finally
            {
                dispose();
            }
        }
    }
}