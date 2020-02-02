namespace SqlStreamStore
{
    using System;
    using System.Threading.Tasks;
    using Microsoft.Data.SqlClient;
    using SqlStreamStore.Infrastructure;
    using SqlStreamStore.TestUtils.MsSql;

    public class MsSqlStreamStoreV3Fixture : IStreamStoreFixture
    {
        private readonly Action _onDispose;
        private bool _preparedPreviously;
        private readonly MsSqlStreamStoreV3Settings _settings;

        public MsSqlStreamStoreV3Fixture(
            string schema,
            DockerMsSqlServerDatabase dockerInstance,
            string databaseName,
            Action onDispose)
        {
            _onDispose = onDispose;

            DatabaseName = databaseName;
            var connectionStringBuilder = dockerInstance.CreateConnectionStringBuilder();
            connectionStringBuilder.MultipleActiveResultSets = true;
            connectionStringBuilder.InitialCatalog = DatabaseName;
            var connectionString = connectionStringBuilder.ToString();

            _settings = new MsSqlStreamStoreV3Settings(connectionString)
            {
                Schema = schema,
                GetUtcNow = () => GetUtcNow(),
                DisableDeletionTracking = false
            };
        }

        public void Dispose()
        {
            Store.Dispose();
            MsSqlStreamStoreV3 = null;
            _onDispose();
        }

        public string DatabaseName { get; }

        public IStreamStore Store => MsSqlStreamStoreV3;

        public MsSqlStreamStoreV3 MsSqlStreamStoreV3 { get; private set; }

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
            MsSqlStreamStoreV3 = new MsSqlStreamStoreV3(_settings);

            await MsSqlStreamStoreV3.CreateSchemaIfNotExists();
            if (_preparedPreviously)
            { 
                using (var connection = new SqlConnection(_settings.ConnectionString))
                {
                    connection.Open();

                    var schema = _settings.Schema;

                    var commandText = $"DELETE FROM [{schema}].[Messages]";
                    using(var command = new SqlCommand(commandText, connection))
                    {
                        command.ExecuteNonQuery();
                    }

                    commandText = $"DELETE FROM [{schema}].[Streams]";
                    using(var command = new SqlCommand(commandText, connection))
                    {
                        command.ExecuteNonQuery();
                    }

                    commandText = $"DBCC CHECKIDENT ('[{schema}].[Streams]', RESEED, 0);";
                    using(var command = new SqlCommand(commandText, connection))
                    {
                        command.ExecuteNonQuery();
                    }

                    commandText = $"DBCC CHECKIDENT ('[{schema}].[Messages]', RESEED, -1);";
                    using(var command = new SqlCommand(commandText, connection))
                    {
                        command.ExecuteNonQuery();
                    }
                }
            }

            _preparedPreviously = true;
        }
    }
}