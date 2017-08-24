namespace SqlStreamStore.Subscriptions
{
    using System.Threading.Tasks;
    using SqlStreamStore.Streams;

    /// <summary>
    ///      Represents a delegate that is invoked when a stream message has been received in a subscription.
    /// </summary>
    /// <param name="subscription">
    ///      The source subscription.
    /// </param>
    /// <param name="streamMessage">
    ///     The stream message.</param>
    /// <returns>A task that represents the asynchronous handling of the stream message.</returns>
    public delegate Task AllStreamMessageReceived(IAllStreamSubscription subscription, StreamMessage streamMessage);
}
