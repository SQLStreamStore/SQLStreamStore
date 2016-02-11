namespace Cedar.EventStore.Subscriptions
{
    using System;

    public delegate void SubscriptionDropped(string reason, Exception ex);
}