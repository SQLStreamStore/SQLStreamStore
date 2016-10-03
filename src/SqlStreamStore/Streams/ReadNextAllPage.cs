namespace SqlStreamStore.Streams
{
    using System.Threading;
    using System.Threading.Tasks;

    public delegate Task<ReadAllPage> ReadNextAllPage(long nextPosition, CancellationToken cancellationToken);
}