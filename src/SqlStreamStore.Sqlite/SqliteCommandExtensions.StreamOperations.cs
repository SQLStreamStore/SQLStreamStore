namespace SqlStreamStore
{
    using Microsoft.Data.Sqlite;

    public class StreamOperations
    {
        private readonly SqliteCommand _command;
        private readonly string _streamId;

        public StreamOperations(SqliteCommand command, string streamId)
        {
            _command = command;
            _streamId = streamId;
        }
    }
}