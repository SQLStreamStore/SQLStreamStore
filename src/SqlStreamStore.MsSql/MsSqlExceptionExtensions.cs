namespace SqlStreamStore
{
    using Microsoft.Data.SqlClient;

    internal static class MsSqlExceptionExtensions
    {
        internal static bool IsUniqueConstraintViolationOnIndex(this SqlException exception, string indexName)
        {
            return IsUniqueConstraintViolation(exception) && exception.Message.Contains($"'{indexName}'");
        }

        internal static bool IsUniqueConstraintViolation(this SqlException exception)
        {
            // https://technet.microsoft.com/en-us/library/aa258747%28v=sql.80%29.aspx
            return exception.Number == 2601;
        }
    }
}