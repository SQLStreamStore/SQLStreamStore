namespace SqlStreamStore.Streams
{
    using System.Threading;
    using System.Threading.Tasks;

    public delegate Task<ListStreamsPage> ListNextStreamsPage(
        string continuationToken,
        CancellationToken cancellationToken = default);
}