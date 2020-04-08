namespace SqlStreamStore
{
    using System;
    using System.Linq;
    using System.Reflection;
    using Microsoft.Data.Sqlite;

    internal static class Extensions
    {
        public static T ExecuteScalar<T>(this SqliteCommand command, T defaultValue = default)
        {
            var obj = command.ExecuteScalar();
            if(obj == DBNull.Value || obj == null)
            {
                return defaultValue;
            }
            
            var resolvedType = typeof(T);
            Type actionableType = resolvedType;
            if(resolvedType.IsGenericType)
            {
                // get the generic type's "T" type.
                actionableType = resolvedType.GetGenericArguments().First();
            }
            
            var methodInfo = actionableType.GetMethod("Parse", BindingFlags.Public | BindingFlags.Static, null,
                new [] { typeof(string) }, null);

            if(methodInfo != null)
            {
                return (T)methodInfo.Invoke(obj, new[] { obj.ToString() });
            }

            return (T) obj;
        }

        public static T ReadScalar<T>(this SqliteDataReader reader, int columnIndex, T defaultValue = default)
        {
            var obj = reader.GetValue(columnIndex);
            if(obj == DBNull.Value || obj == null)
            {
                return defaultValue;
            }
            var resolvedType = typeof(T);
            Type actionableType = resolvedType;
            if(resolvedType.IsGenericType)
            {
                // get the generic type's "T" type.
                actionableType = resolvedType.GetGenericArguments().First();
            }
            
            var methodInfo = actionableType.GetMethod("Parse", BindingFlags.Public | BindingFlags.Static, null,
                new [] { typeof(string) }, null);

            if(methodInfo != null)
            {
                return (T)methodInfo.Invoke(obj, new[] { obj.ToString() });
            }

            return (T) obj;
        }
    }
}