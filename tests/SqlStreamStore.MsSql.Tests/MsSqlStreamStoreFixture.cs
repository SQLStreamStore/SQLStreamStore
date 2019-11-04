namespace SqlStreamStore
{
    using System;
    using System.Threading.Tasks;
    using SqlStreamStore.Infrastructure;

    public sealed class MsSqlStreamStoreFixture : IStreamStoreFixture
    {
        private readonly bool _createSchema;
        public readonly string ConnectionString;
        private readonly string _schema;
        private readonly string _databaseName;
        private readonly DockerMsSqlServerDatabase _databaseInstance;
        private readonly MsSqlStreamStoreSettings _settings;

        private MsSqlStreamStoreFixture(
            string schema,
            bool createSchema = true)
        {
            _schema = schema;
            _createSchema = createSchema;
            _databaseName = $"sss-v3-{Guid.NewGuid():n}";
            _databaseInstance = new DockerMsSqlServerDatabase(_databaseName);

            var connectionStringBuilder = _databaseInstance.CreateConnectionStringBuilder();
            connectionStringBuilder.MultipleActiveResultSets = true;
            connectionStringBuilder.InitialCatalog = _databaseName;
            ConnectionString = connectionStringBuilder.ToString();
            _settings = new MsSqlStreamStoreSettings(ConnectionString)
            {
                Schema = _schema,
                GetUtcNow = () => GetUtcNow(),
            };
        }

        public MsSqlStreamStore Store { get; private set; }

        public GetUtcNow GetUtcNow { get; set; } = SystemClock.GetUtcNow;

        public long MinPosition { get; set; } = 0;

        public int MaxSubscriptionCount { get; set; } = 500;

        public bool DisableDeletionTracking
        {
            get => throw new NotSupportedException();
            set => throw new NotSupportedException();
        }

        IStreamStore IStreamStoreFixture.Store => Store;

        private async Task Init(bool createV1Schema = false)
        {
            await _databaseInstance.CreateDatabase();
            Store = new MsSqlStreamStore(_settings);
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
                createSchema:createSchema);
            await fixture.Init();
            return fixture;
        }

        public static async Task<MsSqlStreamStoreFixture> CreateWithV1Schema(string schema = "dbo")
        {
            var fixture = new MsSqlStreamStoreFixture(schema);
            await fixture.Init(createV1Schema: true);
            return fixture;
        }

        public void Dispose()
        {
            Store?.Dispose();
            Store = null;
        }
    }
}