namespace SqlStreamStore
{
    using System;
    using System.Data.SqlClient;
    using System.Threading.Tasks;

    public class MsSqlStreamStoreV3Fixture : StreamStoreAcceptanceTestFixture
    {
        public readonly string ConnectionString;
        private readonly string _schema;
        private readonly bool _disableDeletionTracking;
        private readonly bool _deleteDatabaseOnDispose;
        private readonly string _databaseName;
        private readonly DockerMsSqlServerDatabase _databaseInstance;

        public MsSqlStreamStoreV3Fixture(
            string schema,
            bool disableDeletionTracking = false,
            bool deleteDatabaseOnDispose = true)
        {
            _schema = schema;
            _disableDeletionTracking = disableDeletionTracking;
            _deleteDatabaseOnDispose = deleteDatabaseOnDispose;
            _databaseName = $"sss-v3-{Guid.NewGuid():n}";
            _databaseInstance = new DockerMsSqlServerDatabase(_databaseName);

            ConnectionString = CreateConnectionString();
        }

        public override long MinPosition => 0;

        public override int MaxSubscriptionCount => 500;

        public override async Task<IStreamStore> GetStreamStore()
        {
            await _databaseInstance.CreateDatabase();

            return await GetStreamStore(_schema);
        }

        public async Task<IStreamStore> GetStreamStore(string schema)
        {
            var settings = new MsSqlStreamStoreV3Settings(ConnectionString)
            {
                Schema = schema,
                GetUtcNow = () => GetUtcNow(),
                DisableDeletionTracking = _disableDeletionTracking
            };
            var store = new MsSqlStreamStoreV3(settings);
            await store.CreateSchemaIfNotExists();

            return store;
        }

        public async Task<MsSqlStreamStoreV3> GetUninitializedStreamStore()
        {
            await _databaseInstance.CreateDatabase();

            return new MsSqlStreamStoreV3(new MsSqlStreamStoreV3Settings(ConnectionString)
            {
                Schema = _schema,
                GetUtcNow = () => GetUtcNow()
            });
        }

        public async Task<MsSqlStreamStoreV3> GetMsSqlStreamStore()
        {
            await _databaseInstance.CreateDatabase();
            var settings = new MsSqlStreamStoreV3Settings(ConnectionString)
            {
                Schema = _schema,
                GetUtcNow = () => GetUtcNow()
            };

            var store = new MsSqlStreamStoreV3(settings);
            await store.CreateSchemaIfNotExists();
            return store;
        }

        public override void Dispose()
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

        private string CreateConnectionString()
        {
            var connectionStringBuilder = _databaseInstance.CreateConnectionStringBuilder();
            connectionStringBuilder.MultipleActiveResultSets = true;
            connectionStringBuilder.InitialCatalog = _databaseName;

            return connectionStringBuilder.ToString();
        }
    }
}