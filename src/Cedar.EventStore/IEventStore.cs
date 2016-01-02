namespace Cedar.EventStore
{
    using System;
    using System.Threading;
    using System.Threading.Tasks;

    public interface IEventStore : IDisposable
    {
        Task AppendToStream(
            string streamId,
            int expectedVersion,
            NewStreamEvent[] events,
            CancellationToken cancellationToken = default(CancellationToken));

        Task DeleteStream(
            string streamId,
            int expectedVersion = ExpectedVersion.Any,
            CancellationToken cancellationToken = default(CancellationToken));

        Task<AllEventsPage> ReadAll(
            string checkpoint,
            int maxCount,
            ReadDirection direction = ReadDirection.Forward,
            CancellationToken cancellationToken = default(CancellationToken));

        Task<StreamEventsPage> ReadStream(
            string streamId,
            int start,
            int count,
            ReadDirection direction = ReadDirection.Forward,
            CancellationToken cancellationToken = default(CancellationToken));

        Task<IStreamSubscription> SubscribeToStream(
            string streamId,
            EventReceived eventReceived,
            SubscriptionDropped subscriptionDropped,
            CancellationToken cancellationToken = default(CancellationToken));

        string StartCheckpoint { get; }

        string EndCheckpoint { get; }
    }

    public delegate Task EventReceived(StreamEvent streamEvent);

    public delegate void SubscriptionDropped(string reason, Exception ex);
}