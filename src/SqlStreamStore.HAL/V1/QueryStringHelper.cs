namespace SqlStreamStore.V1
{
    using System;
    using System.Collections.Generic;
    using Microsoft.AspNetCore.Http;
    using Microsoft.Extensions.Primitives;

    internal static class QueryStringHelper
    {
        private static readonly char[] s_delimiter = { '=' };
        private static readonly QueryString s_almostEmpty = new QueryString("?");

        public static Dictionary<string, StringValues> ParseQueryString(QueryString queryString)
        {
            var state = new Dictionary<string, StringValues>();

            if(queryString == QueryString.Empty || queryString == s_almostEmpty)
            {
                return state;
            }

            var qs = queryString.Value;

            if(qs[0] == '?')
            {
                qs = qs.Substring(1);
            }

            foreach(var pair in qs.Split('&'))
            {
                var parts = pair.Split(s_delimiter, 2);
                var key = Uri.UnescapeDataString(parts[0].Replace('+', ' '));

                state.TryGetValue(key, out var values);

                if(parts.Length == 1)
                {
                    state[key] = values;
                }
                else
                {
                    var value = Uri.UnescapeDataString(parts[1].Replace('+', ' '));

                    state[key] = StringValues.Concat(value, values);
                }
            }

            return state;
        }
    }
}