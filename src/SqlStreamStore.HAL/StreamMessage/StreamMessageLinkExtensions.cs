namespace SqlStreamStore.HAL.StreamMessage
{
    using SqlStreamStore.Streams;

    internal static class StreamMessageLinkExtensions
    {
        public static Links StreamMessageNavigation(
            this Links links,
            StreamMessage message,
            ReadStreamMessageByStreamVersionOperation operation)
        {
            links.Add(Constants.Relations.First, $"{StreamId(operation)}/0");

            if(operation.StreamVersion > 0)
            {
                links.Add(Constants.Relations.Previous, $"{StreamId(operation)}/{operation.StreamVersion - 1}");
            }

            if(message.StreamId != default)
            {
                links.Add(Constants.Relations.Next, $"{StreamId(operation)}/{operation.StreamVersion + 1}");
            }

            return links.Add(Constants.Relations.Last, $"{StreamId(operation)}/-1")
                .Add(
                    Constants.Relations.Feed,
                    Links.FormatBackwardLink(
                        StreamId(operation),
                        Constants.MaxCount,
                        StreamVersion.End,
                        false),
                    operation.StreamId)
                .Add(
                    Constants.Relations.Message,
                    $"{StreamId(operation)}/{operation.StreamVersion}",
                    $"{operation.StreamId}@{operation.StreamVersion}").Self();
        }

        private static string StreamId(ReadStreamMessageByStreamVersionOperation operation)
            => $"streams/{operation.StreamId}";
    }
}