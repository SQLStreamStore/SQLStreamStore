namespace SqlStreamStore
{
    using System;
    using System.Threading.Tasks;
    using SqlStreamStore.Infrastructure;

    public class SQLiteStreamStoreFixture : IStreamStoreFixture
    {
        private readonly Action _onDispose;
        private readonly SQLiteStreamStoreSettings _settings;

        public SQLiteStreamStoreFixture(Action onDispose)
        {
            _onDispose = onDispose;
            var connectionString = $"Data Source={System.IO.Path.GetTempFileName()};Version=3;Pooling=True;Max Pool Size=100;";

            _settings = new SQLiteStreamStoreSettings(connectionString)
            {
                GetUtcNow = () => GetUtcNow()
            };
        }

        public IStreamStore Store => SQLiteStreamStore;
        public SQLiteStreamStore SQLiteStreamStore { get; private set; }
        public GetUtcNow GetUtcNow { get; set; } = SystemClock.GetUtcNow;
        public long MinPosition { get; set; } = 0;
        public int MaxSubscriptionCount { get; set; } = 100;
        public bool DisableDeletionTracking 
        { 
            get => throw new NotSupportedException();
            set => throw new NotSupportedException();
        }

        bool _notPrepared = true;
        public async Task Prepare()
        {
            SQLiteStreamStore = new SQLiteStreamStore(_settings);
            if (_notPrepared)
            {
                await SQLiteStreamStore.CreateSchema();
            }
            _notPrepared = false;
        }
 
        public void Dispose()
        {
            Store.Dispose();
            SQLiteStreamStore = null;
            _onDispose();
        }
   }
}