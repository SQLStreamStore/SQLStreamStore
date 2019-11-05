namespace SqlStreamStore
{
    using System;
    using System.Threading.Tasks;
    using global::MySql.Data.MySqlClient;
    using SqlStreamStore.Infrastructure;
    using SqlStreamStore.MySql;

    public class MySqlStreamStoreFixture : IStreamStoreFixture
    {
        private readonly Action _onDispose;
        private bool _preparedPreviously;
        private readonly MySqlStreamStoreSettings _settings;

        public MySqlStreamStoreFixture(
            MySqlDockerDatabaseManager dockerInstance,
            string databaseName,
            Action onDispose)
        {
            _onDispose = onDispose;
            DatabaseName = databaseName;
            var connectionString = dockerInstance.ConnectionString;

            _settings = new MySqlStreamStoreSettings(connectionString)
            {
                GetUtcNow = () => GetUtcNow(),
                DisableDeletionTracking = false
                ScavengeAsynchronously = false,
                AppendDeadlockRetryAttempts = 25
            };
        }

        public void Dispose()
        {
            Store.Dispose();
            MySqlStreamStore = null;
            _onDispose();
        }

        public string DatabaseName { get; }

        public IStreamStore Store => MySqlStreamStore;

        public MySqlStreamStore MySqlStreamStore { get; private set; }

        public GetUtcNow GetUtcNow { get; set; } = SystemClock.GetUtcNow;

        public long MinPosition { get; set; } = 0;

        public int MaxSubscriptionCount { get; set; } = 500;

        public bool DisableDeletionTracking
        {
            get => _settings.DisableDeletionTracking;
            set => _settings.DisableDeletionTracking = value;
        }

        public async Task Prepare()
        {
            _settings.DisableDeletionTracking = false;
            MySqlStreamStore = new MySqlStreamStore(_settings);

            await MySqlStreamStore.CreateSchemaIfNotExists();
            if (_preparedPreviously)
            {
                using (var connection = new MySqlConnection(_settings.ConnectionString))
                {
                    connection.Open();

                    var commandText = "DELETE FROM messages";
                    using (var command = new MySqlCommand(commandText, connection))
                    {
                        command.ExecuteNonQuery();
                    }

                    commandText = "DELETE FROM streams";
                    using (var command = new MySqlCommand(commandText, connection))
                    {
                        command.ExecuteNonQuery();
                    }

                    commandText = "ALTER TABLE streams AUTO_INCREMENT = 1";
                    using (var command = new MySqlCommand(commandText, connection))
                    {
                        command.ExecuteNonQuery();
                    }

                    commandText = "ALTER TABLE messages AUTO_INCREMENT = 1";
                    using (var command = new MySqlCommand(commandText, connection))
                    {
                        command.ExecuteNonQuery();
                    }
                }
            }

            _preparedPreviously = true;
        }
    }
}
