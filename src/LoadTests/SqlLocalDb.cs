namespace LoadTests
{
    using System;
    using System.Data.SqlClient;
    using System.Threading.Tasks;
    using MartinCostello.SqlLocalDb;
    using SqlStreamStore;

    public class SqlLocalDb : IDisposable
    {
        private readonly ISqlLocalDbInstanceInfo _instance;
        private readonly ISqlLocalDbInstanceManager _manager;
        private MsSqlStreamStoreV3 _msSqlStreamStoreV3;
        private readonly SqlLocalDbApi _localDb;
        private readonly string _databaseName = Guid.NewGuid().ToString();

        public SqlLocalDb()
        {
            _localDb = new SqlLocalDbApi();
            _instance = _localDb.GetOrCreateInstance("SSS-LoadTests");
            _manager = _instance.Manage();
            Initialize().Wait();
        }

        private async Task Initialize()
        {
            if (!_instance.IsRunning)
            {
                _manager.Start();
            }
            
            using (var connection = _instance.CreateConnection())
            {
                await connection.OpenAsync();
                var tempPath = Environment.GetEnvironmentVariable("Temp");
                var createDatabase = $"CREATE DATABASE [{_databaseName}] on (name='{_databaseName}', "
                                     + $"filename='{tempPath}\\{_databaseName}.mdf')";
                using (var command = new SqlCommand(createDatabase, connection))
                {
                    await command.ExecuteNonQueryAsync();
                }
            }

            var sqlConnectionStringBuilder = _instance.CreateConnectionStringBuilder();
            sqlConnectionStringBuilder.InitialCatalog = _databaseName;
            ConnectionString = sqlConnectionStringBuilder.ToString();
            var settings = new MsSqlStreamStoreV3Settings(ConnectionString);
            _msSqlStreamStoreV3 = new MsSqlStreamStoreV3(settings);
            await _msSqlStreamStoreV3.CreateSchemaIfNotExists();
        }

        public IStreamStore StreamStore => _msSqlStreamStoreV3;

        public string ConnectionString { get; private set; }

        public void Dispose()
        {
            _msSqlStreamStoreV3.Dispose();
            _localDb.Dispose();
        }
    }
}