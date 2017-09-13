namespace SqlStreamStore.InMemory
{
    using System.Threading.Tasks;
    using SqlStreamStore;

    public class InMemoryStreamStoreFixture : StreamStoreAcceptanceTestFixture
    {
        public override Task<IStreamStore> GetStreamStore()
        {
            IStreamStore streamStore = new InMemoryStreamStore(() => GetUtcNow());
            return Task.FromResult(streamStore);
        }

        public override long MinPosition => 0;
    }
}