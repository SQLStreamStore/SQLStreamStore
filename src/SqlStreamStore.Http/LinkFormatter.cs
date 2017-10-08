namespace SqlStreamStore
{
    using SqlStreamStore.Streams;

    internal static class Constants
    {
        internal static class Direction
        {
            public const int Forwards = 1;
            public const int Backwards = -1;
        }
        internal static class Headers
        {
            public const string HeadPosition = "SSS-HeadPosition";
            public const string ExpectedVersion = "SSS-ExpectedVersion";

        }
    }

    internal static class LinkFormatter
    {
        public static string AllHead => "/stream";

        public static string Stream(StreamId streamId)
            => $"/streams/{streamId}";

        public static string ReadAllForwards(long fromPositionInclusive, int maxCount, bool prefetchJsonData)
            => ReadAll(
                fromPositionInclusive,
                maxCount,
                prefetchJsonData,
                Constants.Direction.Forwards);
        
        public static string ReadAllBackwards(long fromPositionInclusive, int maxCount, bool prefetchJsonData)
            => ReadAll(
                fromPositionInclusive,
                maxCount,
                prefetchJsonData,
                Constants.Direction.Backwards);

        private static string ReadAll(long fromPositionInclusive, int maxCount, bool prefetchJsonData, int direction)
            =>
                $"{AllHead}?d={(direction == Constants.Direction.Forwards ? "f" : "b")}"
                + $"&m={maxCount}&p={fromPositionInclusive}{(prefetchJsonData ? "&e=1" : string.Empty)}";
    }
}