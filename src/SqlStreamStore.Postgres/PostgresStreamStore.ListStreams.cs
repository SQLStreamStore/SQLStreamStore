namespace SqlStreamStore
{
    using System.Threading;
    using System.Threading.Tasks;
    using SqlStreamStore.Streams;

    public partial class PostgresStreamStore
    {
        public override async Task<ListStreamsPage> ListStreams(
            string startsWith,
            int startingAt = 0,
            int maxCount = 100,
            CancellationToken cancellationToken = default)
        {
            throw new System.NotImplementedException();
        }
    }
}