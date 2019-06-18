namespace SqlStreamStore.V1
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using Microsoft.Extensions.Primitives;

    internal struct CacheControl : IEquatable<CacheControl>
    {
        private readonly string[] _values;

        public static readonly CacheControl NoCache = new CacheControl(
            "max-age=0",
            "no-cache",
            "must-revalidate");

        public static readonly CacheControl OneYear = new CacheControl("max-age=31536000");

        private CacheControl(params string[] values)
        {
            if(values == null)
                throw new ArgumentNullException(nameof(values));
            _values = values;
        }

        public bool Equals(CacheControl other) => _values
            .OrderBy(value => value)
            .SequenceEqual(other._values.OrderBy(value => value), StringComparer.OrdinalIgnoreCase);

        public override bool Equals(object obj) => obj is CacheControl other && Equals(other);

        public override int GetHashCode() => _values
            .Aggregate(397, (previous, value) => previous ^ (value.GetHashCode() * 397));

        public static bool operator ==(CacheControl left, CacheControl right) => left.Equals(right);
        public static bool operator !=(CacheControl left, CacheControl right) => !left.Equals(right);
        public static implicit operator string[](CacheControl cacheControl) => cacheControl._values;
        public static implicit operator StringValues(CacheControl cacheControl) =>
            new StringValues(cacheControl._values);
        public static implicit operator KeyValuePair<string, string[]>(CacheControl cacheControl)
            => new KeyValuePair<string, string[]>(Constants.Headers.CacheControl, cacheControl._values);

    }
}