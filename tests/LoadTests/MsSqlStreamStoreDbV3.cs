namespace LoadTests
{
    using System;
    using System.Threading.Tasks;
    using Microsoft.Data.SqlClient;
    using SqlStreamStore;

    public class MsSqlStreamStoreDbV3 : IDisposable
    {
        public readonly string ConnectionString;
        private readonly string _schema;
        private readonly bool _disableDeletionTracking;
        private readonly string _databaseNameOverride;
        private readonly bool _deleteDatabaseOnDispose;
        private readonly string _databaseName;
        private readonly DockerMsSqlServerDatabase _databaseInstance;

        public MsSqlStreamStoreDbV3(
            string schema,
            bool disableDeletionTracking = false,
            string databaseNameOverride = null,
            bool deleteDatabaseOnDispose = true)
        {
            _schema = schema;
            _disableDeletionTracking = disableDeletionTracking;
            _databaseNameOverride = databaseNameOverride;
            _deleteDatabaseOnDispose = deleteDatabaseOnDispose;
            _databaseName = databaseNameOverride ?? $"sss-v3-{Guid.NewGuid():n}";
            _databaseInstance = new DockerMsSqlServerDatabase(_databaseName);

            ConnectionString = CreateConnectionString();
        }

        public async Task<IStreamStore> GetStreamStore()
        {
            await CreateDatabase();

            return await GetStreamStore(_schema);
        }

        private async Task<IStreamStore> GetStreamStore(string schema)
        {
            var settings = new MsSqlStreamStoreV3Settings(ConnectionString)
            {
                Schema = schema,
                DisableDeletionTracking = _disableDeletionTracking
            };
            var store = new MsSqlStreamStoreV3(settings);
            await store.CreateSchemaIfNotExists();

            return store;
        }

        public void Dispose()
        {
            if (!_deleteDatabaseOnDispose)
            {
                return;
            }

            SqlConnection.ClearAllPools();

            using (var connection = _databaseInstance.CreateConnection())
            {
                connection.Open();
                using (var command = new SqlCommand($"ALTER DATABASE [{_databaseName}] SET SINGLE_USER WITH ROLLBACK IMMEDIATE", connection))
                {
                    command.ExecuteNonQuery();
                }
                using (var command = new SqlCommand($"DROP DATABASE [{_databaseName}]", connection))
                {
                    command.ExecuteNonQuery();
                }
            }
        }

        private async Task CreateDatabase()
        {
            if (_databaseNameOverride == null)
            {
                await _databaseInstance.CreateDatabase();
            }
        }

        private string CreateConnectionString()
        {
            var connectionStringBuilder = _databaseInstance.CreateConnectionStringBuilder();
            connectionStringBuilder.MultipleActiveResultSets = true;
            connectionStringBuilder.InitialCatalog = _databaseName;

            return connectionStringBuilder.ToString();
        }
    }
}