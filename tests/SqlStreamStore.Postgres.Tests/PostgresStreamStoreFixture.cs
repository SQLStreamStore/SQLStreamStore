namespace SqlStreamStore
{
    using System;
    using System.Threading.Tasks;
    using Npgsql;
    using SqlStreamStore.Infrastructure;
    using SqlStreamStore.TestUtils.Postgres;

    public class PostgresStreamStoreFixture : IStreamStoreFixture<PostgresReadAllPage>
    {
        private readonly Action _onDispose;
        private bool _preparedPreviously;
        private readonly PostgresStreamStoreSettings _settings;

        public PostgresStreamStoreFixture(
            string schema,
            PostgresContainer dockerInstance,
            string databaseName,
            Action onDispose)
        {
            _onDispose = onDispose;

            DatabaseName = databaseName;
            var connectionString = dockerInstance.GenerateConnectionString();

            _settings = new PostgresStreamStoreSettings(connectionString)
            {
                Schema = schema,
                GetUtcNow = () => GetUtcNow(),
                DisableDeletionTracking = false,
                ScavengeAsynchronously = false,
            };
        }

        public void Dispose()
        {
            Store.Dispose();
            PostgresStreamStore = null;
            _onDispose();
        }

        public string DatabaseName { get; }

        public IStreamStore<PostgresReadAllPage> Store => PostgresStreamStore;

        public PostgresStreamStore PostgresStreamStore { get; private set; }

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
            _settings.DisableDeletionTracking = false;
            PostgresStreamStore = new PostgresStreamStore(_settings);

            await PostgresStreamStore.CreateSchemaIfNotExists();
            if (_preparedPreviously)
            {
                using (var connection = new NpgsqlConnection(_settings.ConnectionString))
                {
                    connection.Open();

                    var schema = _settings.Schema;

                    var commandText = $"DELETE FROM {schema}.messages";
                    using (var command = new NpgsqlCommand(commandText, connection))
                    {
                        command.ExecuteNonQuery();
                    }

                    commandText = $"DELETE FROM {schema}.streams";
                    using (var command = new NpgsqlCommand(commandText, connection))
                    {
                        command.ExecuteNonQuery();
                    }

                    commandText = $"ALTER SEQUENCE {schema}.streams_seq RESTART WITH 1;";
                    using (var command = new NpgsqlCommand(commandText, connection))
                    {
                        command.ExecuteNonQuery();
                    }

                    commandText = $"ALTER SEQUENCE {schema}.messages_seq RESTART WITH 0;";
                    using (var command = new NpgsqlCommand(commandText, connection))
                    {
                        command.ExecuteNonQuery();
                    }
                }
            }

            _preparedPreviously = true;
        }
    }
}