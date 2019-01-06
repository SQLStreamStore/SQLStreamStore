namespace SqlStreamStore
{
    using System.Threading.Tasks;
    using SqlStreamStore.Infrastructure;

    public sealed class MsSqlStreamStoreFixture2 : IStreamStoreFixture
    {
        private readonly MsSqlStreamStoreFixture _fixture;

        private MsSqlStreamStoreFixture2(
            MsSqlStreamStoreFixture fixture,
            MsSqlStreamStore store,
            GetUtcNow getUtcNow)
        {
            _fixture = fixture;
            Store = store;
            GetUtcNow = getUtcNow;
        }

        public static async Task<MsSqlStreamStoreFixture2> Create(
            string schema = "dbo",
            bool deleteDatabaseOnDispose = true,
            GetUtcNow getUtcNow = null,
            string databaseName = null)
        {
            getUtcNow = getUtcNow ?? SystemClock.GetUtcNow;
            var fixture = new MsSqlStreamStoreFixture(schema, deleteDatabaseOnDispose, databaseName)
            {
                GetUtcNow = getUtcNow
            };
            var streamStore = await fixture.GetMsSqlStreamStore();
            return new MsSqlStreamStoreFixture2(fixture, streamStore, getUtcNow);
        }

        public static async Task<MsSqlStreamStoreFixture2> CreateUninitialized(
            string schema = "dbo",
            bool deleteDatabaseOnDispose = true,
            GetUtcNow getUtcNow = null,
            string databaseName = null)
        {
            getUtcNow = getUtcNow ?? SystemClock.GetUtcNow;
            var fixture = new MsSqlStreamStoreFixture(schema, deleteDatabaseOnDispose, databaseName)
            {
                GetUtcNow = getUtcNow
            };
            var streamStore = await fixture.GetUninitializedStreamStore();
            return new MsSqlStreamStoreFixture2(fixture, streamStore, getUtcNow);
        }

        public static async Task<MsSqlStreamStoreFixture2> CreateWithV1Schema(
            string schema = "dbo",
            bool deleteDatabaseOnDispose = true,
            GetUtcNow getUtcNow = null,
            string databaseName = null)
        {
            getUtcNow = getUtcNow ?? SystemClock.GetUtcNow;
            var fixture = new MsSqlStreamStoreFixture(schema, deleteDatabaseOnDispose, databaseName)
            {
                GetUtcNow = getUtcNow
            };
            var streamStore = await fixture.GetStreamStore_v1Schema();
            return new MsSqlStreamStoreFixture2(fixture, streamStore, getUtcNow);
        }

        public void Dispose()
        {
            Store.Dispose();
            _fixture.Dispose();
        }

        IStreamStore IStreamStoreFixture.Store => Store;

        public MsSqlStreamStore Store { get; }

        public GetUtcNow GetUtcNow
        {
            get => _fixture.GetUtcNow;
            set => _fixture.GetUtcNow = value;
        }
    }
}