namespace SqlStreamStore
{
    using Npgsql;

    internal static class NpgsqlExceptionExtensions
    {
        public static bool IsUniqueConstraintViolation(this NpgsqlException exception, string indexName)
            => exception.IsUniqueConstraintViolation() && exception.Message.Contains(indexName);

        public static bool IsUniqueConstraintViolation(this NpgsqlException exception) => exception.ErrorCode == 23505;
    }
}