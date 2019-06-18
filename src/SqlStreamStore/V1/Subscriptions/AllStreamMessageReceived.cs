namespace SqlStreamStore.V1.Subscriptions
{
    using System.Threading;
    using System.Threading.Tasks;
    using SqlStreamStore.V1.Streams;

    /// <summary>
    ///      Represents a delegate that is invoked when a stream message has been received in a subscription.
    /// </summary>
    /// <param name="subscription">
    ///      The source subscription.
    /// </param>
    /// <param name="streamMessage">
    ///     The stream message.
    /// </param>
    /// <param name="cancellationToken">
    ///     The cancellation instruction.
    /// </param>
    /// <returns>A task that represents the asynchronous handling of the stream message.</returns>
    public delegate Task AllStreamMessageReceived(
        IAllStreamSubscription subscription,
        StreamMessage streamMessage,
        CancellationToken cancellationToken);
}
