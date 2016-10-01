namespace SqlStreamStore
{
    using EnsureThat;
    using SqlStreamStore.Subscriptions;

    public static class ReadonlyStreamExtensions
    {
        /// <summary>
        ///     Subsribes to the all stream.
        /// </summary>
        /// <param name="streamMessageReceived">
        ///     A delegate that is invoked when a message is available. If an exception is thrown, the subscription
        ///     is terminated.
        /// </param>
        /// <param name="subscriptionDropped">
        ///     A delegate that is invoked when a the subscription fails.
        /// </param>
        /// <param name="name">
        ///     The name of the subscription used for logging. Optional.
        /// </param>
        /// <returns>
        ///     An <see cref="IStreamSubscription"/> that represents the subscription. Dispose to stop the subscription.
        /// </returns>
        public static IAllStreamSubscription SubscribeToAllFromStart(
            this IReadonlyStreamStore streamStore,
            StreamMessageReceived streamMessageReceived,
            SubscriptionDropped subscriptionDropped = null,
            string name = null)
        {
            Ensure.That(streamStore, nameof(streamStore)).IsNotNull();

            return streamStore.SubscribeToAll(null, streamMessageReceived, subscriptionDropped, name);
        }
    }
}