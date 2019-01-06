namespace SqlStreamStore
{
    using System;
    using System.Data.SqlClient;
    using System.Threading.Tasks;
    using SqlStreamStore.Infrastructure;

    public sealed class MsSqlStreamStoreV3Fixture2 : IStreamStoreFixture
    {
        private readonly bool _createSchema;
        public readonly string ConnectionString;
        private readonly string _schema;
        private readonly bool _disableDeletionTracking;
        private readonly bool _deleteDatabaseOnDispose;
        private readonly string _databaseName;
        private readonly DockerMsSqlServerDatabase _databaseInstance;

        private MsSqlStreamStoreV3Fixture2(
            string schema,
            bool disableDeletionTracking = false,
            bool deleteDatabaseOnDispose = true,
            bool createSchema = true)
        {
            _schema = schema;
            _disableDeletionTracking = disableDeletionTracking;
            _deleteDatabaseOnDispose = deleteDatabaseOnDispose;
            _createSchema = createSchema;
            _databaseName = $"sss-v3-{Guid.NewGuid():n}";
            _databaseInstance = new DockerMsSqlServerDatabase(_databaseName);

            var connectionStringBuilder = _databaseInstance.CreateConnectionStringBuilder();
            connectionStringBuilder.MultipleActiveResultSets = true;
            connectionStringBuilder.InitialCatalog = _databaseName;
            ConnectionString = connectionStringBuilder.ToString();
        }

        IStreamStore IStreamStoreFixture.Store => Store;

        public MsSqlStreamStoreV3 Store { get; private set; }

        public GetUtcNow GetUtcNow { get; set; } = SystemClock.GetUtcNow;

        public long MinPosition { get; set; } = 0;

        public int MaxSubscriptionCount { get; set; } = 500;

        private async Task Init()
        {
            await _databaseInstance.CreateDatabase();
            var settings = new MsSqlStreamStoreV3Settings(ConnectionString)
            {
                Schema = _schema,
                GetUtcNow = () => GetUtcNow(),
                DisableDeletionTracking = _disableDeletionTracking
            };
            Store = new MsSqlStreamStoreV3(settings);
            if(_createSchema)
            {
                await Store.CreateSchemaIfNotExists();
            }
        }

        public static async Task<MsSqlStreamStoreV3Fixture2> Create(
            string schema = "dbo",
            bool createSchema = true,
            bool deleteDatabaseOnDispose = true)
        {
            var fixture = new MsSqlStreamStoreV3Fixture2(
                schema,
                false,
                deleteDatabaseOnDispose,
                createSchema);
            await fixture.Init();
            return fixture;
        }

        public void Dispose()
        {
            Store?.Dispose();

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
    }
}