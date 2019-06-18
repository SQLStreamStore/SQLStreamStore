namespace SqlStreamStore.V1
{
    using System.Threading;
    using System.Threading.Tasks;
    using SqlStreamStore.V1.Streams;

    partial class MsSqlStreamStore
    {
        protected override Task<ListStreamsPage> ListStreamsInternal(
            Pattern pattern,
            int maxCount,
            string continuationToken,
            ListNextStreamsPage listNextStreamsPage,
            CancellationToken cancellationToken)
        {
            throw new System.NotImplementedException("ListStreams is only available in MsSqlStreamStoreV3 onwards.");
        }
    }
}