namespace SqlStreamStore.InMemory
{
    using System;
    using SqlStreamStore.Infrastructure;

    public class InMemoryStreamStoreFixture : IStreamStoreFixture
    {
        public InMemoryStreamStoreFixture()
        {
            Store = new InMemoryStreamStore(() => GetUtcNow());  
        }

        public void Dispose()
        {
            Store.Dispose();
        }

        public IStreamStore Store { get; }

        public GetUtcNow GetUtcNow { get; set; } = SystemClock.GetUtcNow;

        public long MinPosition { get; set; } = 0;

        public int MaxSubscriptionCount { get; set; } = 500;

        public bool DisableDeletionTracking
        {
            get => throw new NotSupportedException();
            set => throw new NotSupportedException();
        }
    }
}