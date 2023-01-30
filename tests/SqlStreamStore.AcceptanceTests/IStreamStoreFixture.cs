namespace SqlStreamStore
{
    using System;
    using SqlStreamStore.Infrastructure;
    using SqlStreamStore.Streams;

    public interface IStreamStoreFixture<out TReadPage> : IDisposable where TReadPage : IReadAllPage
    {
        IStreamStore<TReadPage> Store { get; }

        GetUtcNow GetUtcNow { get; set; }

        long MinPosition { get; set; }

        int MaxSubscriptionCount { get; set; }

        bool DisableDeletionTracking { get; set; }
    }
}