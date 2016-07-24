namespace StreamStore
{
    using System;

    public interface IStreamSubscription : IDisposable
    {
        string Name { get; }

        string StreamId { get; }

        int LastVersion { get; }  
    }
}