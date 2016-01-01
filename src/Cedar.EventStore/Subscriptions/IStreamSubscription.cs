namespace Cedar.EventStore
{
    using System;

    public interface IStreamSubscription : IDisposable
    {
        string StreamId { get; }

        int LastEventNumber { get; }  
    }
}