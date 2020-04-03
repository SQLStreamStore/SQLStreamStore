namespace SqlStreamStore
{
    using System;
    using System.Threading.Tasks;
    using Microsoft.Data.Sqlite;
    //using Microsoft.Data.Sqlite;
    using SqlStreamStore.Infrastructure;

    public class SQLiteStreamStoreFixture : IStreamStoreFixture
    {
        private readonly SQLiteStreamStoreSettings _settings;

        public SQLiteStreamStoreFixture()
        {
            var connectionString = $"Data Source={System.IO.Path.GetTempFileName()};Cache=Shared;";

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

        
        private bool _preparedPreviously = false;
        public Task Prepare()
        {
            SQLiteStreamStore = new SQLiteStreamStore(_settings);
            SQLiteStreamStore.CreateSchemaIfNotExists();
            if(_preparedPreviously)
            {
                using(var connection = new SqliteConnection(_settings.ConnectionString))
                using (var command = connection.CreateCommand())
                {
                    connection.Open();
                    command.CommandText = @"DELETE FROM messages;
                                            DELETE FROM streams;
                                            DELETE FROM sqlite_sequence WHERE name='streams';
                                            DELETE FROM sqlite_sequence WHERE name='messages'";
                    command.ExecuteNonQuery();
                }
            }

            _preparedPreviously = true;
            
            return Task.CompletedTask;
        }
 
        public void Dispose()
        {
            Store?.Dispose();
            SQLiteStreamStore = null;
        }
   }
}