namespace SqlStreamStore
{
    using System;
    using System.Threading.Tasks;
    using Microsoft.Data.SqlClient;
    using SqlStreamStore.Infrastructure;

    public sealed class MsSqlStreamStoreV3Fixture : IStreamStoreFixture
    {
        private readonly bool _createSchema;
        public readonly string ConnectionString;
        private readonly string _schema;
        private readonly bool _deleteDatabaseOnDispose;
        private readonly string _databaseName;
        private readonly DockerMsSqlServerDatabase _databaseInstance;
        private readonly MsSqlStreamStoreV3Settings _settings;

        private MsSqlStreamStoreV3Fixture(
            string schema,
            bool disableDeletionTracking = false,
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

            _settings = new MsSqlStreamStoreV3Settings(ConnectionString)
            {
                Schema = _schema,
                GetUtcNow = () => GetUtcNow(),
                DisableDeletionTracking = disableDeletionTracking
            };
        }

        IStreamStore IStreamStoreFixture.Store => Store;

        public MsSqlStreamStoreV3 Store { get; private set; }

        public GetUtcNow GetUtcNow { get; set; } = SystemClock.GetUtcNow;

        public long MinPosition { get; set; } = 0;

        public int MaxSubscriptionCount { get; set; } = 500;

        public bool DisableDeletionTracking
        {
            get => _settings.DisableDeletionTracking;
            set => _settings.DisableDeletionTracking = value;
        }

        private async Task Init()
        {
            await _databaseInstance.CreateDatabase();
            Store = new MsSqlStreamStoreV3(_settings);
            if(_createSchema)
            {
                await Store.CreateSchemaIfNotExists();
            }
        }

        public static async Task<MsSqlStreamStoreV3Fixture> Create(
            string schema = "dbo",
            bool createSchema = true,
            bool deleteDatabaseOnDispose = true)
        {
            var fixture = new MsSqlStreamStoreV3Fixture(
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

            if(!_deleteDatabaseOnDispose)
            {
                return;
            }

            SqlConnection.ClearAllPools();
            using(var connection = _databaseInstance.CreateConnection())
            {
                connection.Open();
                using(var command =
                    new SqlCommand($"ALTER DATABASE [{_databaseName}] SET SINGLE_USER WITH ROLLBACK IMMEDIATE",
                        connection))
                {
                    command.ExecuteNonQuery();
                }

                using(var command = new SqlCommand($"DROP DATABASE [{_databaseName}]", connection))
                {
                    command.ExecuteNonQuery();
                }
            }
        }
    }
}