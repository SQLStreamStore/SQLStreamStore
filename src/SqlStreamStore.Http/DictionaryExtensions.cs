namespace SqlStreamStore
{
    using System;
    using System.Collections.Generic;
    using System.Text;

    internal static class DictionaryExtensions
    {
        public static string ToUrlFormEncoded(this IDictionary<string, string> queryString)
        {
            var builder = new StringBuilder();

            foreach(var pair in queryString)
            {
                if(builder.Length > 0)
                {
                    builder.Append('&');
                }

                builder.Append(UrlEncode(pair.Key)).Append('=').Append(UrlEncode(pair.Value));
            }

            return builder.ToString();

            string UrlEncode(string value) => string.IsNullOrEmpty(value)
                ? string.Empty
                : Uri.EscapeDataString(value);
        }
    }
}