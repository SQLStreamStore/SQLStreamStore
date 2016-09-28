namespace SqlStreamStore.Subscriptions
{
    using System;

    public delegate void SubscriptionDropped(SubscriptionDroppedReason reason, Exception ex = null);
}