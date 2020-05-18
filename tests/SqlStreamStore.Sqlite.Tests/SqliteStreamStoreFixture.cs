namespace SqlStreamStore
{
    using System.Threading.Tasks;
    using SqlStreamStore.Infrastructure;

    public class SqliteStreamStoreFixture : IStreamStoreFixture
    {
        private bool _preparedPreviously = false;
        private readonly SqliteStreamStoreSettings _settings;

        public SqliteStreamStoreFixture()
        {
            var connectionString = $"Data Source={System.IO.Path.GetTempFileName()};Cache=Shared;";

            _settings = new SqliteStreamStoreSettings(connectionString)
            {
                GetUtcNow = () => GetUtcNow(),
            };
        }

        public void Dispose()
        {
            if(Store != null)
            {
                Store.Dispose();
                SqliteStreamStore = null;
            }
        }

        public IStreamStore Store => SqliteStreamStore;

        public SqliteStreamStore SqliteStreamStore { get; private set; }

        public GetUtcNow GetUtcNow { get; set; } = SystemClock.GetUtcNow;

        public long MinPosition { get; set; } = 0;

        public int MaxSubscriptionCount { get; set; } = 100;

        public bool DisableDeletionTracking
        {
            get => _settings.DisableDeletionTracking;
            set => _settings.DisableDeletionTracking = value;
        }

        public Task Prepare()
        {
            _settings.DisableDeletionTracking = false;
            SqliteStreamStore = new SqliteStreamStore(_settings);

            SqliteStreamStore.CreateSchemaIfNotExists();
            if (_preparedPreviously)
            {
                using(var connection = SqliteStreamStore.OpenConnection(false))
                using(var command = connection.CreateCommand())
                {
                    command.CommandText = @"DELETE FROM messages;
                                            DELETE FROM streams;
                                            UPDATE sqlite_sequence SET seq = 0 WHERE name = 'messages';
                                            UPDATE sqlite_sequence SET seq = 0 WHERE name = 'streams';";
                    command.ExecuteNonQuery();
                }
            }

            _preparedPreviously = true;

            return Task.CompletedTask;
        }
    }
}