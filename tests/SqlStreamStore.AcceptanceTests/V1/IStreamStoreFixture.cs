namespace SqlStreamStore.V1
{
    using System;
    using SqlStreamStore.V1.Infrastructure;

    public interface IStreamStoreFixture : IDisposable
    {
        IStreamStore Store { get; }

        GetUtcNow GetUtcNow { get; set; }

        long MinPosition { get; set; }

        int MaxSubscriptionCount { get; set; }

        bool DisableDeletionTracking { get; set; }
    }
}