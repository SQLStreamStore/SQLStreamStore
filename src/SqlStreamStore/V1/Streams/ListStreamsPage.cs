namespace SqlStreamStore.V1.Streams
{
    using System.Threading;
    using System.Threading.Tasks;
    using SqlStreamStore.V1.Imports.Ensure.That;

    /// <summary>
    /// Represents the results of listing stream ids.
    /// </summary>
    public sealed class ListStreamsPage
    {
        /// <summary>
        /// A list of stream ids that matched a certain <see cref="Pattern" />
        /// </summary>
        public string[] StreamIds { get; }

        /// <summary>
        /// A continuation token used to retrieve the next page of results.
        /// </summary>
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

        /// <summary>
        /// Call this method to automatically get the next <see cref="ListStreamsPage"/>
        /// </summary>
        /// <param name="cancellationToken"></param>
        /// <returns>A <see cref="ListStreamsPage" /></returns>
        public Task<ListStreamsPage> Next(CancellationToken cancellationToken = default) 
            => _listNextStreamsPage(ContinuationToken, cancellationToken);
    }
}