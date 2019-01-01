namespace SqlStreamStore.HAL
{
    internal static class LinksExtensions
    {
        public static Links Index(this Links links) =>
            links.Add(Constants.Relations.Index, string.Empty, "Index");

        public static Links Find(this Links links)
            => links.Add(Constants.Relations.Find, "streams/{streamId}", "Find a Stream");

        public static Links Browse(this Links links)
            => links.Add(Constants.Relations.Browse, "streams{?p,t,m}", "Browse Streams");
    }
}