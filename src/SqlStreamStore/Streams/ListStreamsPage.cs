namespace SqlStreamStore.Streams
{
    using System.Threading;
    using System.Threading.Tasks;
    using SqlStreamStore.Imports.Ensure.That;

    public sealed class ListStreamsPage
    {
        public string[] StreamIds { get; }
        private readonly int _startingAt;
        private readonly ListNextStreamsPage _listNextStreamsPage;

        public ListStreamsPage(
            int startingAt,
            string[] streamIds,
            ListNextStreamsPage listNextStreamsPage)
        {
            Ensure.That(startingAt).IsGte(0);
            Ensure.That(streamIds).IsNotNull();
            Ensure.That(listNextStreamsPage).IsNotNull();

            StreamIds = streamIds;
            _startingAt = startingAt;
            _listNextStreamsPage = listNextStreamsPage;
        }

        public Task<ListStreamsPage> Next(CancellationToken cancellationToken = default) 
            => _listNextStreamsPage(_startingAt, cancellationToken);
    }
}