namespace SqlStreamStore
{
    using System;
    using System.Threading.Tasks;
    using Microsoft.Data.SqlClient;
    using SqlStreamStore.Infrastructure;
    using SqlStreamStore.TestUtils.MsSql;

    public class MsSqlStreamStoreFixture : IStreamStoreFixture
    {
        private readonly Action _onDispose;
        private bool _preparedPreviously;
        private readonly MsSqlStreamStoreSettings _settings;

        public MsSqlStreamStoreFixture(
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
            ConnectionString = connectionStringBuilder.ToString();

            _settings = new MsSqlStreamStoreSettings(ConnectionString)
            {
                Schema = schema,
                GetUtcNow = () => GetUtcNow(),
            };
        }

        public string DatabaseName { get; }

        public IStreamStore Store => MsSqlStreamStore;

        public MsSqlStreamStore MsSqlStreamStore { get; private set; }

        public GetUtcNow GetUtcNow { get; set; } = SystemClock.GetUtcNow;

        public long MinPosition { get; set; } = 0;

        public int MaxSubscriptionCount { get; set; } = 500;

        public string ConnectionString { get; }

        public bool DisableDeletionTracking
        {
            get => throw new NotSupportedException();
            set => throw new NotSupportedException();
        }

        public async Task Prepare()
        {
            MsSqlStreamStore = new MsSqlStreamStore(_settings);

            await MsSqlStreamStore.CreateSchema();
            if (_preparedPreviously)
            {
                using (var connection = new SqlConnection(_settings.ConnectionString))
                {
                    connection.Open();

                    var schema = _settings.Schema;

                    var commandText = $"DELETE FROM [{schema}].[Messages]";
                    using (var command = new SqlCommand(commandText, connection))
                    {
                        command.ExecuteNonQuery();
                    }

                    commandText = $"DELETE FROM [{schema}].[Streams]";
                    using (var command = new SqlCommand(commandText, connection))
                    {
                        command.ExecuteNonQuery();
                    }

                    commandText = $"DBCC CHECKIDENT ('[{schema}].[Streams]', RESEED, 0);";
                    using (var command = new SqlCommand(commandText, connection))
                    {
                        command.ExecuteNonQuery();
                    }

                    commandText = $"DBCC CHECKIDENT ('[{schema}].[Messages]', RESEED, -1);";
                    using (var command = new SqlCommand(commandText, connection))
                    {
                        command.ExecuteNonQuery();
                    }
                }
            }

            _preparedPreviously = true;
        }

        public void Dispose()
        {
            Store.Dispose();
            MsSqlStreamStore = null;
            _onDispose();
        }
    }
}