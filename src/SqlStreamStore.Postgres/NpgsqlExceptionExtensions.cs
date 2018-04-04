namespace SqlStreamStore
{
    using Npgsql;

    internal static class NpgsqlExceptionExtensions
    {
        public static bool IsWrongExpectedVersion(this PostgresException exception)
            => exception.MessageText.Equals("WrongExpectedVersion");
    }
}