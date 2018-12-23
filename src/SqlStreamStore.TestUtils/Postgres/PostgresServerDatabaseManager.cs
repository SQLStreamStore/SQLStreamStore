namespace SqlStreamStore.Postgres
{
    using Npgsql;
    using Xunit.Abstractions;

    public class PostgresServerDatabaseManager : PostgresDatabaseManager
    {
        public override string ConnectionString { get; }

        public PostgresServerDatabaseManager(ITestOutputHelper testOutputHelper, string databaseName, string connectionString)
            : base(testOutputHelper, databaseName)
        {
            ConnectionString = new NpgsqlConnectionStringBuilder(connectionString)
            {
                Database = DatabaseName
            }.ConnectionString;
        }
    }
}