namespace SqlStreamStore
{
    using System;
    using System.Threading.Tasks;
    using SqlStreamStore.Infrastructure;

    public class SQLiteStreamStoreFixture : IStreamStoreFixture
    {
        private readonly Action _onDispose;
        private readonly SQLiteStreamStoreSettings _settings;

        public SQLiteStreamStoreFixture(
            string databaseName, 
            string connectionString,
            Action onDispose)
        {
            _onDispose = onDispose;

            DatabaseName = databaseName;

            _settings = new SQLiteStreamStoreSettings(connectionString)
            {
                GetUtcNow = () => GetUtcNow(),
                DisableDeletionTracking = false,
                ScavengeAsynchronously = false,
            };
        }

        public void Dispose()
        {
            Store.Dispose();
            SQLiteStreamStore = null;
            _onDispose();
        }

        public string DatabaseName { get; }

        public IStreamStore Store => SQLiteStreamStore;

        public SQLiteStreamStore SQLiteStreamStore { get; private set; }
        public GetUtcNow GetUtcNow { get; set; } = SystemClock.GetUtcNow;
        public long MinPosition { get; set; } = 0;
        public int MaxSubscriptionCount { get; set; } = 100;
        public bool DisableDeletionTracking 
        { 
            get => _settings.DisableDeletionTracking;
            set => _settings.DisableDeletionTracking = value;
        }

        public async Task Prepare()
        {
        }
    }
}