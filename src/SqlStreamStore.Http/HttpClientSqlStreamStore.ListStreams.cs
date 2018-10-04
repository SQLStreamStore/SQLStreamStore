namespace SqlStreamStore
{
    using System.Threading;
    using System.Threading.Tasks;
    using SqlStreamStore.Streams;

    partial class HttpClientSqlStreamStore
    {
        public Task<ListStreamsPage> ListStreams(int startingAt = 0, int maxCount = 100, CancellationToken cancellationToken = default)
        {
            throw new System.NotImplementedException();
        }

        public Task<ListStreamsPage> ListStreams(
            string startsWith,
            int startingAt = 0,
            int maxCount = 100,
            CancellationToken cancellationToken = default)
        {
            throw new System.NotImplementedException();
        }
    }
}