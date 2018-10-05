namespace SqlStreamStore
{
    using System.Threading;
    using System.Threading.Tasks;
    using SqlStreamStore.Streams;

    partial class HttpClientSqlStreamStore
    {
        public Task<ListStreamsPage> ListStreams(int maxCount = 100, string continuationToken = default, CancellationToken cancellationToken = default)
        {
            throw new System.NotImplementedException();
        }

        public Task<ListStreamsPage> ListStreams(
            string startsWith,
            int maxCount = 100,
            string continuationToken = default,
            CancellationToken cancellationToken = default)
        {
            throw new System.NotImplementedException();
        }
    }
}