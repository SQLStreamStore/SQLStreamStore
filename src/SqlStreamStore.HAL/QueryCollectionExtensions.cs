namespace SqlStreamStore.HAL
{
    using System;
    using Microsoft.AspNetCore.Http;
    using Microsoft.Extensions.Primitives;

    internal static class QueryCollectionExtensions
    {
        public static bool TryGetValueCaseInsensitive(this IQueryCollection query, char key, out StringValues values)
        {
            if(query == null)
            {
                throw new ArgumentNullException(nameof(query));
            }

            return char.IsUpper(key)
                ? query.TryGetValue(key.ToString(), out values)
                  || query.TryGetValue(char.ToLower(key).ToString(), out values)
                : query.TryGetValue(key.ToString(), out values)
                  || query.TryGetValue(char.ToUpper(key).ToString(), out values);
        }
    }
}