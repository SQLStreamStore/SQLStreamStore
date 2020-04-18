namespace SqlStreamStore
{
    using System.Threading;
    using System.Threading.Tasks;
    using Microsoft.Data.Sqlite;

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

        public async Task<long?> ReadHeadPosition(CancellationToken cancellationToken = default)
        {
            return(await _connection.Streams("$position")
                .Properties(cancellationToken))
                .Position;
        }
    }
}