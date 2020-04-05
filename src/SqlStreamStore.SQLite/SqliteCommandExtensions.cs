namespace SqlStreamStore
{
    using System;
    using Microsoft.Data.Sqlite;

    internal static class SqliteCommandExtensions
    {
        public static T ExecuteScalar<T>(this SqliteCommand command)
        {
            var result = command.ExecuteScalar();
            return !(result == null || result == DBNull.Value)
                ? (T) result
                : default;
        }
    }
}