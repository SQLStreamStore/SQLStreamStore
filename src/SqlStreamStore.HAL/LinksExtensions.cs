namespace SqlStreamStore.HAL
{
    internal static class LinksExtensions
    {
        public static Links Index(this Links links) =>
            links.Add(Constants.Relations.Index, string.Empty, "Index");

        public static Links Find(this Links links)
            => links.Add(Constants.Relations.Find, $"{Constants.Paths.Streams}/{{streamId}}", "Find a Stream");

        public static Links Browse(this Links links)
            => links.Add(Constants.Relations.Browse, $"{Constants.Paths.Streams}{{?p,t,m}}", "Browse Streams");
    }
}