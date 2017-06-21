namespace LoadTests
{
    using System;
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.Threading;
    using System.Threading.Tasks;
    using EasyConsole;
    using SqlStreamStore;
    using SqlStreamStore.Streams;

    public class AllStreamSubscription : LoadTest {
        protected override async Task RunAsync(CancellationToken ct) {
            Output.WriteLine("");
            Output.WriteLine(ConsoleColor.Green,
                "Subscribes to multiple individual streams and appends to each of them in turn, multiple times.");
            Output.WriteLine("");

            var streamStore = GetStore();

            var streamCount = Input.ReadInt(
                "Number of streams: ",
                1,
                1000);

            var messageCountPerStream = Input.ReadInt(
                "Number of messages per stream: ",
                1,
                1000);

            var messageSize = Input.ReadInt(
                "Size of message in bytes: ",
                1,
                4096);

            var subscriberCount = Input.ReadInt(
                "Number of all stream subscribers: ",
                1,
                100);

            var total = messageCountPerStream * streamCount * subscriberCount;

            var jasonData = new String('a', messageSize);

            var subscriptions = new List<IDisposable>();
            int messagesReceived = 0;

            for(int i = 0; i < subscriberCount; i++)
            {
                var subscription =
                    streamStore.SubscribeToAll(default(long?),
                        (_, __) =>
                        {
                            Interlocked.Increment(ref messagesReceived);
                            return Task.CompletedTask;
                        });
                subscriptions.Add(subscription);
            }

            var eventNumber = 1;

            var stopwatch = Stopwatch.StartNew();

            for (var version = 0; version < messageCountPerStream; version ++) { 
            for (var stream = 0; stream < streamCount; stream++)
            {
                var newStreamMessage = new NewStreamMessage(Guid.NewGuid(), "type", jasonData, "\"metadata\"");
                await streamStore.AppendToStream($"stream-{stream}", version + ExpectedVersion.NoStream, newStreamMessage, ct);
                eventNumber++;
                Console.Write($"\r> Appended: {version} / {messageCountPerStream} . Received: {messagesReceived} / {total}");
            }}

            while(messagesReceived < total)
            {
                Console.Write($"\r> Appended: {messageCountPerStream} / {messageCountPerStream} . Received: {messagesReceived} / {total}");
                await Task.Delay(100, ct);
            }

            Console.Write($"\r> Appended: {messageCountPerStream} / {messageCountPerStream} . Received: {messagesReceived} / {total}");

            foreach (var subscription in subscriptions)
            {
                subscription.Dispose();
            }
            Output.WriteLine($"> Complete in {stopwatch.Elapsed:c}");
        }
    }

    public class StreamSubscription : LoadTest
    {
        protected override async Task RunAsync(CancellationToken ct)
        {
            Output.WriteLine("");
            Output.WriteLine(ConsoleColor.Green,
                "Subscribes to multiple individual streams and appends to each of them in turn, multiple times.");
            Output.WriteLine("");

            var streamStore = GetStore();

            int numberOfStreams = Input.ReadInt(
                "Number of independent streams, each will have it's own subscriber: ",
                1,
                1000);

            int numberOfAmends = Input.ReadInt("Number of times each stream will have an ammend: ", 1, 1000);

            string jsonData = "{}";

            var subscriptions = new List<IDisposable>();
            int messagesReceived = 0;

            for(int i = 0; i < numberOfStreams; i++)
            {
                var subscription =
                    streamStore.SubscribeToStream($"stream-{i}", null,
                        (_, __, ___) =>
                        {
                            Interlocked.Increment(ref messagesReceived);
                            return Task.CompletedTask;
                        });
                subscriptions.Add(subscription);
            }

            int amendAcount = 0;
            int eventNumber = 1;
            var totalMessages = numberOfAmends * numberOfStreams;
            var stopwatch = Stopwatch.StartNew();

            while (amendAcount < numberOfAmends)
            {
                for(int i = 0; i < numberOfStreams; i++)
                {
                    var newStreamMessages = StreamStoreAcceptanceTests.CreateNewStreamMessages(jsonData, eventNumber);
                    await streamStore.AppendToStream($"stream-{i}", ExpectedVersion.Any, newStreamMessages, ct);
                    eventNumber++;
                }
                amendAcount++;
                Console.Write($"\r> Ammending: {amendAcount} / {numberOfAmends} . Received: {messagesReceived} / {totalMessages}");
            }

            while(messagesReceived < totalMessages)
            {
                Console.Write($"\r> Ammending: {amendAcount} / {numberOfAmends} . Received: {messagesReceived} / {totalMessages}");
                await Task.Delay(100, ct);
            }

            Console.WriteLine($"\r> Ammending: {amendAcount} / {numberOfAmends} . Received: {messagesReceived} / {totalMessages}");

            foreach (var subscription in subscriptions)
            {
                subscription.Dispose();
            }
            Output.WriteLine($"> Complete in {stopwatch.Elapsed:c}");
        }
    }
}