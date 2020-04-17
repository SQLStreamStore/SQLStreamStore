namespace SqlStreamStore
{
    using System.Data;
    using System.Threading.Tasks;
    using Microsoft.Data.Sqlite;

    public static class SqliteCommandExtensions
    {
        private static SqliteStreamStoreSettings _settings;

        public static void WithSettings(SqliteStreamStoreSettings settings) => _settings = settings;
        
        public static AllStreamOperations AllStream(this SqliteConnection connection)
            => new AllStreamOperations(connection, _settings);

        public static StreamOperations Streams(this SqliteConnection connection, string streamId) 
            => new StreamOperations(connection, streamId);

        public static Task<string> GetJsonData(string streamId, int streamVersion)
        {
            using (var connection = new SqliteConnection(_settings.GetConnectionString(true)))
            using(var command = connection.CreateCommand())
            {
                connection.Open();
                
                command.CommandText = @"SELECT messages.json_data
FROM messages
WHERE messages.stream_id_internal = 
(
SELECT streams.id_internal
FROM streams
WHERE streams.id = @streamId)
AND messages.stream_version = @streamVersion";
                command.Parameters.Clear();
                command.Parameters.AddWithValue("@streamId", streamId);
                command.Parameters.AddWithValue("@streamVersion", streamVersion);

                using(var reader = command.ExecuteReader(CommandBehavior.SequentialAccess | CommandBehavior.SingleRow))
                {
                    return Task.FromResult(reader.Read() 
                        ? reader.GetTextReader(0).ReadToEnd() 
                        : default(string));
                }
            }
        }
    }
}