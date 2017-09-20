namespace SqlStreamStore
{
    using MySql.Data.MySqlClient;

    internal static class MySqlExceptionExtensions
    {
        public static bool IsUniqueConstraintViolationOnIndex(this MySqlException exception, string indexName)
            => exception.IsUniqueConstraintViolation() && exception.Message.Contains(indexName);

        public static bool IsUniqueConstraintViolation(this MySqlException exception)
            => exception.Number == 1062;
    }
}