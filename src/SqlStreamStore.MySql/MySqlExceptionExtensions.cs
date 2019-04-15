namespace SqlStreamStore
{
    using MySql.Data.MySqlClient;

    internal static class MySqlExceptionExtensions
    {
        public static bool IsWrongExpectedVersion(this MySqlException exception)
            => exception.Message.Equals("WrongExpectedVersion") || exception.SqlState == "23000";
    }
}