namespace SqlStreamStore.Streams
{
    using System.Threading;
    using System.Threading.Tasks;

    public delegate Task<ListStreamsPage> ListNextStreamsPage(
        int startingAt,
        CancellationToken cancellationToken = default);
}