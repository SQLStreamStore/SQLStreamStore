namespace LoadTests
{
    using System;
    using System.Threading.Tasks;
    using Microsoft.Data.SqlClient;
    using SqlStreamStore;
    using SqlStreamStore.TestUtils.MsSql;

    public class MsSqlStreamStoreDb : IDisposable
    {
        public readonly string ConnectionString;
        private readonly string _schema;
        private readonly bool _deleteDatabaseOnDispose;
        private readonly string _databaseName;
        private readonly SqlServerContainer _containerInstance;

        public MsSqlStreamStoreDb(string schema, bool deleteDatabaseOnDispose = true)
        {
            _schema = schema;
            _deleteDatabaseOnDispose = deleteDatabaseOnDispose;
            _databaseName = $"sss-v2-{Guid.NewGuid():n}";
            _containerInstance = new SqlServerContainer(_databaseName);

            ConnectionString = CreateConnectionString();
        }

        public async Task<IStreamStore> GetStreamStore()
        {
            await CreateDatabase();

            return await GetStreamStore(_schema);
        }

        public async Task<IStreamStore> GetStreamStore(string schema)
        {
            var settings = new MsSqlStreamStoreSettings(ConnectionString)
            {
                Schema = schema,
            };
            var store = new MsSqlStreamStore(settings);
            await store.CreateSchema();

            return store;
        }

        public void Dispose()
        {
            if (!_deleteDatabaseOnDispose)
            {
                return;
            }
            using (var sqlConnection = new SqlConnection(ConnectionString))
            {
                // Fixes: "Cannot drop database because it is currently in use"
                SqlConnection.ClearPool(sqlConnection);
            }
            using (var connection = _containerInstance.CreateConnection())
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
            await _containerInstance.Start();
            await _containerInstance.CreateDatabase();
        }

        private string CreateConnectionString()
        {
            var connectionStringBuilder = _containerInstance.CreateConnectionStringBuilder();
            connectionStringBuilder.MultipleActiveResultSets = true;
            connectionStringBuilder.InitialCatalog = _databaseName;

            return connectionStringBuilder.ToString();
        }
    }
}