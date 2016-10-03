namespace SqlStreamStore.Subscriptions
{
    using System;

    /// <summary>
    ///     A delegate that is invoked when a subscription has dropped.
    /// </summary>
    /// <param name="reason">
    ///     The subscription dropped reason.</param>
    /// <param name="exception">
    ///     The underlying exception that caused the subscription to drop, if one exists.
    /// </param>
    public delegate void SubscriptionDropped(SubscriptionDroppedReason reason, Exception exception = null);
}