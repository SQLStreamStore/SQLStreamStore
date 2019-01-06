namespace SqlStreamStore
{
    using System.Threading.Tasks;
    using SqlStreamStore.Infrastructure;

    public sealed class MsSqlStreamStoreV3Fixture2 : IStreamStoreFixture
    {
        private readonly MsSqlStreamStoreFixture _fixture;

        private MsSqlStreamStoreV3Fixture2(
            MsSqlStreamStoreFixture fixture,
            MsSqlStreamStore store,
            GetUtcNow getUtcNow)
        {
            _fixture = fixture;
            Store = store;
            GetUtcNow = getUtcNow;
        }

        public static async Task<MsSqlStreamStoreV3Fixture2> Create(
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
            return new MsSqlStreamStoreV3Fixture2(fixture, streamStore, getUtcNow);
        }

        public static async Task<MsSqlStreamStoreV3Fixture2> CreateUninitialized(
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
            return new MsSqlStreamStoreV3Fixture2(fixture, streamStore, getUtcNow);
        }

        public static async Task<MsSqlStreamStoreV3Fixture2> CreateWithV1Schema(
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
            return new MsSqlStreamStoreV3Fixture2(fixture, streamStore, getUtcNow);
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