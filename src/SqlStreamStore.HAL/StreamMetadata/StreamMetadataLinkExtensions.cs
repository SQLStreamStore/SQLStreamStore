namespace SqlStreamStore.HAL.StreamMetadata
{
    internal static class StreamMetadataLinkExtensions
    {
        public static Links StreamMetadataNavigation(this Links links, GetStreamMetadataOperation operation)
            => links.StreamMetadataNavigation(operation.StreamId);

        public static Links StreamMetadataNavigation(this Links links, SetStreamMetadataOperation operation)
            => links.StreamMetadataNavigation(operation.StreamId);

        private static Links StreamMetadataNavigation(this Links links, string streamId)
            => links.Add(Constants.Relations.Metadata, $"{Constants.Paths.Streams}/{streamId}/{Constants.Paths.Metadata}").Self()
                .Add(Constants.Relations.Feed, $"{Constants.Paths.Streams}/{streamId}", streamId);
    }
}