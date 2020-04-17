namespace SqlStreamStore
{
    using System;
    using System.Threading;
    using System.Threading.Tasks;
    using Microsoft.Data.Sqlite;
    using SqlStreamStore.Streams;

    public class AllStreamOperations
    {
        private readonly SqliteConnection _connection;
        private readonly SqliteStreamStoreSettings _settings;

        public AllStreamOperations(SqliteConnection connection)
        {
            _connection = connection;
        }
        public AllStreamOperations(SqliteConnection connection, SqliteStreamStoreSettings settings) : this(connection)
        {
            _settings = settings;
        }

        public Task<long?> ReadHeadPosition(CancellationToken cancellationToken = default)
        {
            using(var command = _connection.CreateCommand())
            {
                command.CommandText = @"SELECT MAX([position]) FROM messages;";
                return Task.FromResult(command.ExecuteScalar<long?>());
            }
        }
    }
}