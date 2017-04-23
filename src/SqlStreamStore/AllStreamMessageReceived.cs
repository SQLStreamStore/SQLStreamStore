namespace SqlStreamStore
{
    using System.Threading.Tasks;

    /// <summary>
    ///      Repesents a delegate that is invoked when a stream messages has been received in a subscription.
    /// </summary>
    /// <param name="subscription">
    ///      The source subscription.
    /// </param>
    /// <param name="streamMessage">
    ///     The stream message.</param>
    /// <returns>A task that represents the asynchronous handling of the stream message.</returns>
    public delegate Task AllStreamMessageReceived(IAllStreamSubscription subscription, StreamMessage streamMessage);
}