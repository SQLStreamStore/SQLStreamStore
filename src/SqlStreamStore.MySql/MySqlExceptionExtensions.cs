namespace SqlStreamStore
{
    using MySqlConnector;

    internal static class MySqlExceptionExtensions
    {
        public static bool IsWrongExpectedVersion(this MySqlException exception)
            => exception.Message.Equals("WrongExpectedVersion") || exception.SqlState == "23000";

        public static bool IsDeadlock(this MySqlException exception)
            => exception.Number == 1213;
    }
}