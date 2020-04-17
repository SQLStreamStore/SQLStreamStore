namespace SqlStreamStore
{
    using Microsoft.Data.Sqlite;

    public static class SqliteCommandExtensions
    {
        public static AllStreamOperations AllStream(this SqliteConnection connection)
            => new AllStreamOperations(connection);

        public static StreamOperations Streams(this SqliteCommand command, string streamId) 
            => new StreamOperations(command, streamId);
    }
}