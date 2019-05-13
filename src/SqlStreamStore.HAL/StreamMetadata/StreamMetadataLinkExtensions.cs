namespace SqlStreamStore.HAL.StreamMetadata
{
    internal static class StreamMetadataLinkExtensions
    {
        public static Links StreamMetadataNavigation(this Links links, GetStreamMetadataOperation operation)
            => links.StreamMetadataNavigation(operation.StreamId);

        public static Links StreamMetadataNavigation(this Links links, SetStreamMetadataOperation operation)
            => links.StreamMetadataNavigation(operation.StreamId);

        private static Links StreamMetadataNavigation(this Links links, string streamId)
            => links
                .Add(Constants.Relations.Metadata, LinkFormatter.StreamMetadata(streamId))
                .Self()
                .Add(Constants.Relations.Feed, LinkFormatter.Stream(streamId), streamId);
    }
}