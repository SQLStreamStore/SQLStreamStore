namespace SqlStreamStore
{
    using System;
    using System.Threading.Tasks;
    using SqlStreamStore.Postgres;

    public class PostgresStreamStoreFixture : StreamStoreAcceptanceTestFixture
    {
        public readonly string ConnectionString;
        private readonly string _schema;
        private readonly string _databaseName;

        public PostgresStreamStoreFixture(string schema)
        {
            _schema = schema;

            var uniqueName = Guid.NewGuid().ToString().Replace("-", string.Empty);
            _databaseName = $"StreamStoreTests-{uniqueName}";

            ConnectionString = CreateConnectionString();
        }

        public override async Task<IStreamStore> GetStreamStore()
        {
            await CreateDatabase();

            var settings = new PostgresStreamStoreSettings(ConnectionString)
            {
                Schema = _schema,
                GetUtcNow = () => GetUtcNow()
            };
            var store = new PostgresStreamStore(settings);
            await store.DropAll(ignoreErrors: true);

            return store;
        }

        public async Task<IStreamStore> GetStreamStore(string schema)
        {
            var settings = new PostgresStreamStoreSettings(ConnectionString)
            {
                Schema = schema,
                GetUtcNow = () => GetUtcNow()
            };
            var store = new PostgresStreamStore(settings);

            return store;
        }

        public async Task<PostgresStreamStore> GetPostgresStreamStore()
        {
            await CreateDatabase();

            var settings = new PostgresStreamStoreSettings(ConnectionString)
            {
                Schema = _schema,
                GetUtcNow = () => GetUtcNow()
            };

            var store = new PostgresStreamStore(settings);
            await store.DropAll(ignoreErrors: true);

            return store;
        }

        public override void Dispose()
        {
            //noop
        }

        private async Task CreateDatabase()
        {
            throw new NotImplementedException();
        }

        private string CreateConnectionString()
        {
            throw new NotImplementedException();
        }
    }
}