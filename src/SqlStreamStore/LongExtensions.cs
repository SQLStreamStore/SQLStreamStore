namespace SqlStreamStore
{
    using System;
    using System.Collections.Generic;

    public static class LongExtensions
    {
        public static IEnumerable<long> RangeTo(this long thisInclusive, long endInclusive)
        {
            if (thisInclusive > endInclusive)
            {
                throw new ArgumentException("End value may not be smaller than beginning value", nameof(endInclusive));
            }

            for (var x = thisInclusive; x <= endInclusive; x++)
            {
                yield return x;
            }
        }
    }
}