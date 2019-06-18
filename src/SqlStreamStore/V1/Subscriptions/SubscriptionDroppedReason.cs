namespace SqlStreamStore.V1.Subscriptions
{
    public enum SubscriptionDroppedReason
    {
        /// <summary>
        ///     The subscription was disposed deliberately. The associated exception will be null. You will not usually
        ///     perform any actions as a result of this.
        /// </summary>
        Disposed,

        /// <summary>
        ///     The subscription encountered an error in the subscriber callback. It is your responsibility to check
        ///     the exception to determine whether it is recoverable, and if so, recreate the subscription if
        ///     desired.
        /// </summary>
        SubscriberError,

        /// <summary>
        ///     The subscription encountered an error from the underlying StreamStore. It is your responsibility to check
        ///     the exception to determine whether it is recoverable, and if so, recreate the subscription if 
        ///     desired.
        /// </summary>
        StreamStoreError
    }
}