namespace SqlStreamStore
{
    using System;
    using System.Collections.Generic;
    using SqlStreamStore.Streams;

    internal static class LinkFormatter
    {
        public static string AllHead => "/stream";

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

        public static string Stream(StreamId streamId)
            => $"/streams/{streamId}";

        public static string StreamByMessageId(StreamId streamId, Guid messageId)
            => $"{Stream(streamId)}/{messageId}";

        public static string ReadStreamForwards(
            StreamId streamId,
            int fromVersionInclusive,
            int maxCount,
            bool prefetchJsonData)
            => ReadStream(streamId, fromVersionInclusive, maxCount, prefetchJsonData, Constants.Direction.Forwards);

        public static string ReadStreamBackwards(
            StreamId streamId,
            int fromVersionInclusive,
            int maxCount,
            bool prefetchJsonData)
            => ReadStream(streamId, fromVersionInclusive, maxCount, prefetchJsonData, Constants.Direction.Backwards);

        public static string ListStreams(Pattern pattern, int maxCount)
            => $"/streams?p={pattern.Value}&t={GetPatternTypeArgumentName(pattern)}&m={maxCount}";

        public static string ListStreams(Pattern pattern, int maxCount, string continuationToken)
            => $"{ListStreams(pattern, maxCount)}&c={continuationToken}";

        private static string ReadAll(long fromPositionInclusive, int maxCount, bool prefetchJsonData, int direction)
            => $"{AllHead}?{GetStreamQueryString(fromPositionInclusive, maxCount, prefetchJsonData, direction)}";

        private static string ReadStream(
            StreamId streamId,
            int fromVersionInclusive,
            int maxCount,
            bool prefetchJsonData,
            int direction) =>
            $"{Stream(streamId)}?{GetStreamQueryString(fromVersionInclusive, maxCount, prefetchJsonData, direction)}";

        private static string GetStreamQueryString(long positionOrVersion, int maxCount, bool prefetchJsonData, int direction)
        {
            var queryString = new Dictionary<string, string>
            {
                ["d"] = direction == Constants.Direction.Forwards ? "f" : "b",
                ["p"] = $"{positionOrVersion}",
                ["m"] = $"{maxCount}"
            };

            if(prefetchJsonData)
            {
                queryString["e"] = "1";
            }
            
            return queryString.ToUrlFormEncoded();
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
}