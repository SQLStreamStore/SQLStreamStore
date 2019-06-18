namespace SqlStreamStore.StreamMessages
{
    using SqlStreamStore.StreamMessages.Version;
    using SqlStreamStore.Streams;

    internal static class StreamMessageLinkExtensions
    {
        public static Links StreamMessageNavigation(
            this Links links,
            StreamMessage message,
            ReadStreamMessageByStreamVersionOperation operation)
        {
            links.Add(
                Constants.Relations.First,
                LinkFormatter.StreamMessageByStreamVersion(operation.StreamId, 0));

            if(operation.StreamVersion > 0)
            {
                links.Add(
                    Constants.Relations.Previous,
                    LinkFormatter.StreamMessageByStreamVersion(operation.StreamId, operation.StreamVersion - 1));
            }

            if(message.StreamId != default)
            {
                links.Add(
                    Constants.Relations.Next, 
                    LinkFormatter.StreamMessageByStreamVersion(operation.StreamId, operation.StreamVersion + 1));
            }

            return links
                .Add(
                    Constants.Relations.Last,
                    LinkFormatter.StreamMessageByStreamVersion(operation.StreamId, -1))
                .Add(
                    Constants.Relations.Feed,
                    LinkFormatter.ReadStreamBackwards(
                        operation.StreamId,
                        StreamVersion.End,
                        Constants.MaxCount,
                        false),
                    operation.StreamId)
                .Add(
                    Constants.Relations.Message,
                    LinkFormatter.StreamMessageByStreamVersion(operation.StreamId, operation.StreamVersion),
                    $"{operation.StreamId}@{operation.StreamVersion}")
                .Self();
        }
    }
}