namespace SqlStreamStore
{
    using System;
    using System.Threading;
    using System.Threading.Tasks;
    using Microsoft.Data.SqlClient;
    using SqlStreamStore.Infrastructure;
    using SqlStreamStore.TestUtils.MsSql;

    public class MsSqlStreamStoreV3Fixture : IStreamStoreFixture
    {
        private readonly Action _onDispose;
        private readonly MsSqlStreamStoreV3Settings _settings;

        public MsSqlStreamStoreV3Fixture(
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
        public IStreamStore  StoreFor(string user)
        {
            var connectionStringBuilder = new SqlConnectionStringBuilder(_settings.ConnectionString)
            {
                UserID = user,
                Password = Password(user),
            };
            var connectionString = connectionStringBuilder.ToString();
            var settings = new MsSqlStreamStoreV3Settings(connectionString)
            {
                Schema = _settings.Schema,
                GetUtcNow = () => _settings.GetUtcNow(),
                CommandTimeout = _settings.CommandTimeout,
                CreateStreamStoreNotifier = _settings.CreateStreamStoreNotifier,
                DisableDeletionTracking = _settings.DisableDeletionTracking,
                LogName = _settings.LogName,
            };
            return new MsSqlStreamStoreV3(settings);
        }

        public async Task CreateUser(string user, CancellationToken cancellationToken = default)
        {
            string createUser = $"USE [{DatabaseName}];" +
                                $"IF SUSER_ID('{user}') IS NULL CREATE LOGIN [{user}] WITH PASSWORD='{Password(user)}';" +
                                $"IF DATABASE_PRINCIPAL_ID('{user}') IS NULL CREATE USER [{user}] FOR LOGIN[{user}] ;";

            using(var connection = new SqlConnection(_settings.ConnectionString))
            {
                using(var command = new SqlCommand(createUser, connection))
                {
                    await connection
                        .OpenAsync(cancellationToken)
                        .ConfigureAwait(false);
                    await command
                        .ExecuteScalarAsync(cancellationToken)
                        .ConfigureAwait(false);
                }
            }
        }
        private static string Password(string user) => $"{user}@1EasyPassword";

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
            await MsSqlStreamStoreV3.DropAll();
            await MsSqlStreamStoreV3.CreateSchemaIfNotExists();
        }
    }
}
