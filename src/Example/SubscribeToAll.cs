namespace Example
{
    using System;
    using System.Threading;
    using System.Threading.Tasks;
    using EasyConsole;
    using SqlStreamStore;
    using SqlStreamStore.Streams;
    using SqlStreamStore.Subscriptions;

    public class SubscribeToAll : Example
    {
        private TaskCompletionSource<int> _tcs = new TaskCompletionSource<int>();
        private int _numberOfMessagesToAppend;
        private int _receivedCount;

        protected override async Task RunAsync(CancellationToken cancellationToken)
        {
            Output.WriteLine("");
            Output.WriteLine(ConsoleColor.Green, "Subscribes to the All stream");
            Output.WriteLine("");

            _numberOfMessagesToAppend = Input.ReadInt("Number message to append: ", 1, 1000000);

            var streamStore = GetStore();
            var subscription = streamStore.SubscribeToAll(null, StreamMessageReceived, SubscriptionDropped, IsCaughtUp);

            var streamId = "stream-1";
            for(int i = 0; i < _numberOfMessagesToAppend; i++)
            {
                var newStreamMessage = new NewStreamMessage(Guid.NewGuid(), "message-type", "{ \"name\" : \"value\" }");

                await streamStore.AppendToStream(streamId, ExpectedVersion.Any, newStreamMessage, cancellationToken);
            }
            Output.WriteLine(ConsoleColor.Green, "Waiting for subscriptions to to complete.");

            await _tcs.Task;
            subscription.Dispose();

            Output.WriteLine(ConsoleColor.Green, "Done.");
        }

        private static void IsCaughtUp(bool isCaughtUp)
        {
            Output.WriteLine(ConsoleColor.Yellow, $"Caughtup {isCaughtUp}");
        }

        private static void SubscriptionDropped(IAllStreamSubscription _, SubscriptionDroppedReason reason, Exception exception)
        {
            if(reason == SubscriptionDroppedReason.Disposed)
            {
                Output.WriteLine(ConsoleColor.Yellow, "Subscription disposed");
            }
        }

        private Task StreamMessageReceived(IAllStreamSubscription _, StreamMessage streamMessage)
        {
            Output.WriteLine(ConsoleColor.Yellow, $"Received StreamId={streamMessage.StreamId}, MessageId={streamMessage.MessageId}");
            _receivedCount++;
            if(_receivedCount >= _numberOfMessagesToAppend)
            {
                _tcs.SetResult(0);
            }
            return Task.CompletedTask;
        }
    }
}
