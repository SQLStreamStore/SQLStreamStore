namespace SqlStreamStore.Infrastructure
{
    using System;
    using System.Threading.Tasks;
    using EnsureThat;
    using SqlStreamStore.Streams;
    using SqlStreamStore.Subscriptions;

    public class StreamStoreMirror : IDisposable
    {
        private readonly IStreamStore _destination;
        private readonly IAllStreamSubscription _subscription;

        public StreamStoreMirror(IReadonlyStreamStore source, IStreamStore destination)
        {
            Ensure.That(destination, nameof(destination)).IsNotNull();
            Ensure.That(source, nameof(source)).IsNotNull();

            _subscription = source.SubscribeToAll(null, StreamMessageReceived, SubscriptionDropped);
            _destination = destination;
        }

        private void SubscriptionDropped(IAllStreamSubscription subscription, SubscriptionDroppedReason reason, Exception exception)
        {
            // TODO: re-connect.
        }

        private async Task StreamMessageReceived(IAllStreamSubscription subscription, StreamMessage streamMessage)
        {
            var jsonData = await streamMessage.GetJsonData();
            var newStreamMessage = new NewStreamMessage(streamMessage.MessageId, streamMessage.Type, jsonData, streamMessage.JsonMetadata);
            await _destination.AppendToStream(streamMessage.StreamId, ExpectedVersion.Any, newStreamMessage);
        }

        /// <summary>
        ///     The mirrored store.
        /// </summary>
        public IReadonlyStreamStore Desintation => _destination;

        public void Dispose()
        {
            _subscription?.Dispose();
            _destination?.Dispose();
        }
    }
}