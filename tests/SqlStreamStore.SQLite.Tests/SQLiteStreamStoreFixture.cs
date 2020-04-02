namespace SqlStreamStore
{
    using System;
    using System.Threading.Tasks;
    using SqlStreamStore.Infrastructure;

    public class SQLiteStreamStoreFixture : IStreamStoreFixture
    {
        private readonly SQLiteStreamStoreSettings _settings;

        public SQLiteStreamStoreFixture()
        {
            var connectionString = $"Data Source={System.IO.Path.GetTempFileName()};Version=3;Pooling=True;Max Pool Size=100;";
            //var connectionString = $"Data Source=/home/richard/Desktop/sqlite.db3;Version=3;Pooling=True;Max Pool Size=100;";
            
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

        public async Task Prepare()
        {
            SQLiteStreamStore = new SQLiteStreamStore(_settings);
            await SQLiteStreamStore.CreateSchema();
        }
 
        public void Dispose()
        {
            Store?.Dispose();
            SQLiteStreamStore = null;
        }
   }
}