namespace SqlStreamStore
{
    using System;
    using System.Threading.Tasks;

    public interface IStreamSubscription : IDisposable
    {
        string Name { get; }

        string StreamId { get; }

        int LastVersion { get; }

        Task Started { get; }
    }
}