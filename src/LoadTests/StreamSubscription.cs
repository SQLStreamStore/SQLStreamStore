namespace LoadTests
{
    using System;
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.Threading;
    using System.Threading.Tasks;
    using EasyConsole;
    using SqlStreamStore.Streams;

    public class StreamSubscription : LoadTest
    {
        public override async Task Run(CancellationToken ct)
        {
            Output.WriteLine("");
            Output.WriteLine(ConsoleColor.Green,
                "Subscribes to multiple individual streams and appends to each of them in turn, multiple times.");
            Output.WriteLine("");

            var (streamStore, dispose, _) = await GetStore(ct);

            try
            {

                int numberOfStreams = Input.ReadInt(
                    "Number of independent streams, each will have it's own subscriber: ",
                    1,
                    1000);

                int numberOfAppends = Input.ReadInt("Number of times each stream will have an append: ", 1, 1000);

                string jsonData = "{}";

                var subscriptions = new List<IDisposable>();
                int messagesReceived = 0;

                for(int i = 0; i < numberOfStreams; i++)
                {
                    var subscription = streamStore.SubscribeToStream(
                        $"stream-{i}",
                        StreamVersion.None,
                        (_, __, ___) =>
                        {
                            Interlocked.Increment(ref messagesReceived);
                            return Task.CompletedTask;
                        });
                    subscriptions.Add(subscription);
                }

                int amendAcount = 0;
                int eventNumber = 1;
                var totalMessages = numberOfAppends * numberOfStreams;
                var stopwatch = Stopwatch.StartNew();

                while(amendAcount < numberOfAppends)
                {
                    for(int i = 0; i < numberOfStreams; i++)
                    {
                        var newStreamMessages = MessageFactory.CreateNewStreamMessages(jsonData, eventNumber);
                        await streamStore.AppendToStream($"stream-{i}", ExpectedVersion.Any, newStreamMessages, ct);
                        eventNumber++;
                    }

                    amendAcount++;
                    Console.Write(
                        $"\r> Appending: {amendAcount} / {numberOfAppends} . Received: {messagesReceived} / {totalMessages}");
                }

                while(messagesReceived < totalMessages)
                {
                    Console.Write(
                        $"\r> Appending: {amendAcount} / {numberOfAppends} . Received: {messagesReceived} / {totalMessages}");
                    await Task.Delay(100, ct);
                }

                Console.WriteLine(
                    $"\r> Appending: {amendAcount} / {numberOfAppends} . Received: {messagesReceived} / {totalMessages}");

                foreach(var subscription in subscriptions)
                {
                    subscription.Dispose();
                }

                Output.WriteLine($"> Complete in {stopwatch.Elapsed:c}");
            }
            finally
            {
                dispose();
            }
        }
    }
}