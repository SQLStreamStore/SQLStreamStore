namespace SqlStreamStore
{
    using System;
    using System.Threading;
    using System.Threading.Tasks;
    using SqlStreamStore.Streams;

    public partial class SQLiteStreamStore
    {
        protected override Task<ListStreamsPage> ListStreamsInternal(Pattern pattern, int maxCount, string continuationToken, ListNextStreamsPage listNextStreamsPage, CancellationToken cancellationToken)
        {
            throw new NotImplementedException();
        }
    }
}