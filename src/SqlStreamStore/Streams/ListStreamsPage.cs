namespace SqlStreamStore.Streams
{
    using System.Threading;
    using System.Threading.Tasks;
    using SqlStreamStore.Imports.Ensure.That;

    public sealed class ListStreamsPage
    {
        public string[] StreamIds { get; }
        public string ContinuationToken { get; }
        private readonly ListNextStreamsPage _listNextStreamsPage;

        public ListStreamsPage(
            string continuationToken,
            string[] streamIds,
            ListNextStreamsPage listNextStreamsPage)
        {
            Ensure.That(streamIds).IsNotNull();
            Ensure.That(listNextStreamsPage).IsNotNull();

            StreamIds = streamIds;
            ContinuationToken = continuationToken;
            _listNextStreamsPage = listNextStreamsPage;
        }

        public Task<ListStreamsPage> Next(CancellationToken cancellationToken = default) 
            => _listNextStreamsPage(ContinuationToken, cancellationToken);
    }
}