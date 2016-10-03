namespace SqlStreamStore.Streams
{
    using System.Threading;
    using System.Threading.Tasks;

    public delegate Task<ReadStreamPage> ReadNextStreamPage(int nextVersion, CancellationToken cancellationToken);
}