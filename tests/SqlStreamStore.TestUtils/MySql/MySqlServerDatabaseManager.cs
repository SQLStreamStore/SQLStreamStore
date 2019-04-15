namespace SqlStreamStore.MySql
{
    using global::MySql.Data.MySqlClient;
    using Xunit.Abstractions;

    public class MySqlServerDatabaseManager : MySqlDatabaseManager
    {
        public override string ConnectionString { get; }

        public MySqlServerDatabaseManager(
            ITestOutputHelper testOutputHelper,
            string databaseName,
            string connectionString)
            : base(testOutputHelper, databaseName)
        {
            ConnectionString = new MySqlConnectionStringBuilder(connectionString)
            {
                Database = DatabaseName,
                MaximumPoolSize = 500
            }.ConnectionString;
        }
    }
}