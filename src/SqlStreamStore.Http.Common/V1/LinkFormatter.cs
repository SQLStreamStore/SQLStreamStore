namespace SqlStreamStore.V1
{
    using System;
    using System.Text;
    using SqlStreamStore.V1.Streams;

    // ReSharper disable UnusedMember.Global
    internal static class LinkFormatter
    {
        public static string AllStream() => Constants.Paths.AllStream;
        public static string ReadAllForwards(long fromPositionInclusive, int maxCount, bool prefetchJsonData)
            => ReadAll(
                fromPositionInclusive,
                maxCount,
                prefetchJsonData,
                Constants.ReadDirection.Forwards);

        public static string ReadAllBackwards(long fromPositionInclusive, int maxCount, bool prefetchJsonData)
            => ReadAll(
                fromPositionInclusive,
                maxCount,
                prefetchJsonData,
                Constants.ReadDirection.Backwards);

        public static string Stream(StreamId streamId)
            => Stream(new EncodedStreamId(streamId));

        public static string StreamMessageByMessageId(StreamId streamId, Guid messageId)
            => $"{Stream(streamId)}/{messageId}";

        public static string StreamMessageByStreamVersion(StreamId streamId, int streamVersion)
            => $"{Stream(streamId)}/{streamVersion}";

        public static string AllStreamMessageByPosition(long position)
            => $"{Constants.Paths.AllStream}/{position}";

        public static string ReadStreamForwards(
            StreamId streamId,
            int fromVersionInclusive,
            int maxCount,
            bool prefetchJsonData)
            => ReadStream(streamId, fromVersionInclusive, maxCount, prefetchJsonData, Constants.ReadDirection.Forwards);

        public static string ReadStreamBackwards(
            StreamId streamId,
            int fromVersionInclusive,
            int maxCount,
            bool prefetchJsonData)
            => ReadStream(streamId,
                fromVersionInclusive,
                maxCount,
                prefetchJsonData,
                Constants.ReadDirection.Backwards);

        public static string ListStreams(Pattern pattern, int maxCount)
            => $"{Constants.Paths.Streams}?p={pattern.Value}&t={GetPatternTypeArgumentName(pattern)}&m={maxCount}";

        public static string ListStreams(Pattern pattern, int maxCount, string continuationToken)
            => $"{ListStreams(pattern, maxCount)}&c={continuationToken}";

        public static string Docs(string rel)
            => $"{Constants.Paths.Docs}/{rel}";

        public static string StreamMetadata(StreamId streamId)
            => $"{Stream(streamId)}/{Constants.Paths.Metadata}";

        public static string Index() => string.Empty;

        public static string DocsTemplate()
            => $"{Constants.Paths.Docs}/{{rel}}";

        public static string FindStreamTemplate()
            => $"{Constants.Paths.Streams}/{{streamId}}";

        public static string BrowseStreamsTemplate()
            => $"{Constants.Paths.Streams}{{?p,t,m}}";

        private static string Stream(EncodedStreamId encodedStreamId)
            => $"{Constants.Paths.Streams}/{encodedStreamId}";

        private static string ReadAll(long fromPositionInclusive, int maxCount, bool prefetchJsonData, int direction)
            =>
                $"{Constants.Paths.AllStream}?{GetStreamQueryString(fromPositionInclusive, maxCount, prefetchJsonData, direction)}";

        private static string ReadStream(
            StreamId streamId,
            int fromVersionInclusive,
            int maxCount,
            bool prefetchJsonData,
            int direction) =>
            $"{Stream(streamId)}?{GetStreamQueryString(fromVersionInclusive, maxCount, prefetchJsonData, direction)}";

        private static string GetStreamQueryString(
            long positionOrVersion,
            int maxCount,
            bool prefetchJsonData,
            int direction)
        {
            var builder = new StringBuilder()
                .Append("d=").Append(direction == Constants.ReadDirection.Forwards ? "f" : "b")
                .Append("&p=").Append(positionOrVersion)
                .Append("&m=").Append(maxCount);

            return (prefetchJsonData
                    ? builder.Append("&e=1")
                    : builder)
                .ToString();
        }

        private static string GetPatternTypeArgumentName(Pattern pattern)
        {
            switch(pattern)
            {
                case Pattern.StartingWith _:
                    return "s";
                case Pattern.EndingWith _:
                    return "e";
                default:
                    return string.Empty;
            }
        }
    }
    // ReSharper restore UnusedMember.Global
}