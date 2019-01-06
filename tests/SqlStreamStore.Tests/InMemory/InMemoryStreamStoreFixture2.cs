namespace SqlStreamStore.InMemory
{
    using SqlStreamStore.Infrastructure;

    public class InMemoryStreamStoreFixture2 : IStreamStoreFixture
    {
        public InMemoryStreamStoreFixture2()
        {
            Store = new InMemoryStreamStore(() => GetUtcNow());  
        }

        public void Dispose()
        {
            Store.Dispose();
        }

        public IStreamStore Store { get; }

        public GetUtcNow GetUtcNow { get; set; } = SystemClock.GetUtcNow;
    }
}