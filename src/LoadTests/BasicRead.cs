using EasyConsole;
using SqlStreamStore.Streams;

using System;
using System.Diagnostics;
using System.Threading.Tasks;
using System.Threading;
using Microsoft.Data.SqlClient;
using System.Collections.Generic;

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

                int numberOfStreams = 5;
                int messageJsonDataSize = 2;
                int numberOfMessagesPerAmend = 10;
                int maxCount = 10;

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

                                var newmessages = MessageFactory
                                    .CreateNewStreamMessages(jsonData, messageNumbers);

                                await streamStore.AppendToStream(
                                    $"stream-{j + i}",
                                    ExpectedVersion.Any,
                                    newmessages,
                                    ct);
                                //Console.Write($"> {messageNumbers[numberOfMessagesPerAmend - 1]}");
                            }
                            catch (SqlException ex) when (ex.Number == -2)
                            {
                                // just timeout
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

                ReadAllPage readAllPage = await streamStore.ReadAllForwards(StreamVersion.Start, maxCount, true, ct);
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
