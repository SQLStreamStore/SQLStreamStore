namespace SqlStreamStore
{
    using System;
    using System.Threading.Tasks;
    using SqlStreamStore.Infrastructure;
    using SqlStreamStore.TestUtils.MsSql;

    public class MsSqlStreamStoreFixture : IStreamStoreFixture
    {
        private readonly Action _onDispose;
        private readonly MsSqlStreamStoreSettings _settings;

        public MsSqlStreamStoreFixture(
            string schema,
            SqlServerContainer dockerInstance,
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

            await MsSqlStreamStore.DropAll();
            await MsSqlStreamStore.CreateSchema();
        }

        public void Dispose()
        {
            Store.Dispose();
            MsSqlStreamStore = null;
            _onDispose();
        }
    }
}