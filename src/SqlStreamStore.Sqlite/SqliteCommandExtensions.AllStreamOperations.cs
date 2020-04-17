namespace SqlStreamStore
{
    using Microsoft.Data.Sqlite;

    public class AllStreamOperations
    {
        private readonly SqliteCommand _command;

        public AllStreamOperations(SqliteCommand command)
        {
            _command = command;
        }
    }
}