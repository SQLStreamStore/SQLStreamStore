namespace SqlStreamStore.HAL
{
    using System.Collections;
    using System.Collections.Generic;
    using Microsoft.AspNetCore.Http;
    using Microsoft.Extensions.Primitives;

    internal class CaseSensitiveQueryCollection : IQueryCollection
    {
        private readonly QueryString _queryString;

        private Dictionary<string, StringValues> _state;

        public CaseSensitiveQueryCollection(QueryString queryString)
        {
            _queryString = queryString;
        }

        private Dictionary<string, StringValues> GetState()
            => _state
               ?? (_state = QueryStringHelper.ParseQueryString(_queryString));

        public IEnumerator<KeyValuePair<string, StringValues>> GetEnumerator()
            => GetState().GetEnumerator();

        IEnumerator IEnumerable.GetEnumerator()
            => GetEnumerator();

        public bool ContainsKey(string key)
            => GetState().ContainsKey(key);

        public bool TryGetValue(string key, out StringValues value)
            => GetState().TryGetValue(key, out value);

        public int Count
            => GetState().Count;

        public ICollection<string> Keys
            => GetState().Keys;

        public StringValues this[string key]
            => GetState()[key];
    }
}