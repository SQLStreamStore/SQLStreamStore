namespace Cedar.EventStore
{
    using System;

    public interface IAllStreamSubscription : IDisposable
    {
        string Name { get; }

        string LastCheckpoint { get; }
    }
}