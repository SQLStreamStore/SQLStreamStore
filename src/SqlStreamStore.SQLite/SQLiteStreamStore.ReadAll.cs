namespace SqlStreamStore
{
    using System;
    using System.Data.Common;
    using System.Data.SQLite;
    using System.Threading;
    using System.Threading.Tasks;
    using SqlStreamStore.Streams;

    public partial class SQLiteStreamStore
    {
        protected override Task<ReadAllPage> ReadAllForwardsInternal(long fromPositionExclusive, int maxCount, bool prefetch, ReadNextAllPage readNext, CancellationToken cancellationToken)
        {
            throw new NotImplementedException();
        }

        protected override Task<ReadAllPage> ReadAllBackwardsInternal(long fromPositionExclusive, int maxCount, bool prefetch, ReadNextAllPage readNext, CancellationToken cancellationToken)
        {
            throw new NotImplementedException();
        }

    }

    internal static class SqlDataReaderExtensions
    {
        internal static int? GetNullableInt32(this SQLiteDataReader reader, int ordinal) 
            => reader.IsDBNull(ordinal) 
                ? (int?) null 
                : reader.GetInt32(ordinal);

        internal static int? GetNullableInt32(this DbDataReader reader, int ordinal) 
            => reader.IsDBNull(ordinal) 
                ? (int?) null 
                : reader.GetInt32(ordinal);
    }
}