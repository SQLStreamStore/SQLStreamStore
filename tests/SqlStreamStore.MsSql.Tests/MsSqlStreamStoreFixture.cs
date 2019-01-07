namespace SqlStreamStore
{
    using System;
    using System.Data.SqlClient;
    using System.Threading.Tasks;
    using SqlStreamStore.Infrastructure;

    public sealed class MsSqlStreamStoreFixture : IStreamStoreFixture
    {
        private readonly bool _createSchema;
        public readonly string ConnectionString;
        private readonly string _schema;
        private readonly bool _deleteDatabaseOnDispose;
        private readonly string _databaseName;
        private readonly DockerMsSqlServerDatabase _databaseInstance;

        private MsSqlStreamStoreFixture(
            string schema,
            bool deleteDatabaseOnDispose = true,
            bool createSchema = true)
        {
            _schema = schema;
            _deleteDatabaseOnDispose = deleteDatabaseOnDispose;
            _createSchema = createSchema;
            _databaseName = $"sss-v3-{Guid.NewGuid():n}";
            _databaseInstance = new DockerMsSqlServerDatabase(_databaseName);

            var connectionStringBuilder = _databaseInstance.CreateConnectionStringBuilder();
            connectionStringBuilder.MultipleActiveResultSets = true;
            connectionStringBuilder.InitialCatalog = _databaseName;
            ConnectionString = connectionStringBuilder.ToString();
        }

        public MsSqlStreamStore Store { get; private set; }

        public GetUtcNow GetUtcNow { get; set; } = SystemClock.GetUtcNow;

        public long MinPosition { get; set; } = 0;

        public int MaxSubscriptionCount { get; set; } = 500;

        IStreamStore IStreamStoreFixture.Store => Store;

        private async Task Init(bool createV1Schema = false)
        {
            await _databaseInstance.CreateDatabase();
            var settings = new MsSqlStreamStoreSettings(ConnectionString)
            {
                Schema = _schema,
                GetUtcNow = () => GetUtcNow(),
            };
            Store = new MsSqlStreamStore(settings);
            if (_createSchema)
            {
                if(createV1Schema)
                {
                    await Store.CreateSchema_v1_ForTests();
                }
                else
                {
                    await Store.CreateSchema();
                }
            }
        }

        public static async Task<MsSqlStreamStoreFixture> Create(
            string schema = "dbo",
            bool deleteDatabaseOnDispose = true,
            bool createSchema = true)
        {
            var fixture = new MsSqlStreamStoreFixture(
                schema,
                deleteDatabaseOnDispose: deleteDatabaseOnDispose,
                createSchema:createSchema);
            await fixture.Init();
            return fixture;
        }

        public static async Task<MsSqlStreamStoreFixture> CreateWithV1Schema(
            string schema = "dbo",
            bool deleteDatabaseOnDispose = true)
        {
            var fixture = new MsSqlStreamStoreFixture(
                schema,
                deleteDatabaseOnDispose: deleteDatabaseOnDispose);
            await fixture.Init(createV1Schema: true);
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