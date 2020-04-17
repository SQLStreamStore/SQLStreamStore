namespace SqlStreamStore
{
    using System.Threading;
    using System.Threading.Tasks;
    using Microsoft.Data.Sqlite;
    using SqlStreamStore.Streams;

    public class AllStreamOperations
    {
        private readonly SqliteConnection _connection;

        public AllStreamOperations(SqliteConnection connection)
        {
            _connection = connection;
        }

        public Task<long> ReadHeadPosition(CancellationToken cancellationToken = default)
        {
            using(var command = _connection.CreateCommand())
            {
                command.CommandText = @"SELECT MAX([position])
                                        FROM streams;";
                var result = command.ExecuteScalar(Position.End);
                return Task.FromResult(result);
            }
        }
    }
}