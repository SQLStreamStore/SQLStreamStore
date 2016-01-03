namespace Cedar.EventStore.Streams
{
    using System;

    public delegate void SubscriptionDropped(string reason, Exception ex);
}