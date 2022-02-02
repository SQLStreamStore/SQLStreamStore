namespace SqlStreamStore
{
    using System;
    using SqlStreamStore.Infrastructure;

    public interface IStreamStoreFixture: IDisposable
    {
        IStreamStore<PostgresReadAllPage> Store { get; }

        GetUtcNow GetUtcNow { get; set; }

        long MinPosition { get; set; }

        int MaxSubscriptionCount { get; set; }

        bool DisableDeletionTracking { get; set; }
    }
}