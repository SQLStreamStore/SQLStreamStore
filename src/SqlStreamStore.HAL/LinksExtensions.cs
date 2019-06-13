namespace SqlStreamStore.HAL
{
    internal static class LinksExtensions
    {
        public static Links Index(this Links links) =>
            links.Add(Constants.Relations.Index, LinkFormatter.Index(), "Index");

        public static Links Find(this Links links)
            => links.Add(Constants.Relations.Find, LinkFormatter.FindStreamTemplate(), "Find a Stream");

        public static Links Browse(this Links links)
            => links.Add(Constants.Relations.Browse, LinkFormatter.BrowseStreamsTemplate(), "Browse Streams");
    }
}