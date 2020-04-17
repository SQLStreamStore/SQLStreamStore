namespace SqlStreamStore
{
    using System.Data;
    using System.Threading.Tasks;
    using Microsoft.Data.Sqlite;

    public static class SqliteCommandExtensions
    {
        public static AllStreamOperations AllStream(this SqliteConnection connection)
            => new AllStreamOperations(connection);
        
        public static AllStreamOperations AllStream(this SqliteConnection connection, SqliteStreamStoreSettings settings)
            => new AllStreamOperations(connection, settings);

        public static StreamOperations Streams(this SqliteCommand command, string streamId) 
            => new StreamOperations(command, streamId);

        public static Task<string> GetJsonData(this SqliteStreamStoreSettings settings, string streamId, int streamVersion)
        {
            using (var connection = new SqliteConnection(settings.GetConnectionString(true)))
            using(var command = connection.CreateCommand())
            {
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