namespace SqlStreamStore.Streams
{
    using System.Linq;

    internal static class StreamsLinkExtensions
    {
        public static Links StreamsNavigation(this Links links, ReadStreamPage page, ReadStreamOperation operation)
        {
            var first = LinkFormatter.ReadStreamForwards(
                operation.StreamId,
                StreamVersion.Start,
                operation.MaxCount,
                operation.EmbedPayload);

            var last = LinkFormatter.ReadStreamBackwards(
                operation.StreamId,
                StreamVersion.End,
                operation.MaxCount,
                operation.EmbedPayload);

            links.Add(Constants.Relations.First, first);

            if(operation.Self != first && !page.IsEnd)
            {
                links.Add(
                    Constants.Relations.Previous,
                    LinkFormatter.ReadStreamBackwards(
                        operation.StreamId,
                        page.Messages.Min(m => m.StreamVersion) - 1,
                        operation.MaxCount,
                        operation.EmbedPayload));
            }

            links.Add(Constants.Relations.Feed, operation.Self, operation.StreamId).Self();

            if(operation.Self != last && !page.IsEnd)
            {
                links.Add(
                    Constants.Relations.Next,
                    LinkFormatter.ReadStreamForwards(
                        operation.StreamId,
                        page.Messages.Max(m => m.StreamVersion) + 1,
                        operation.MaxCount,
                        operation.EmbedPayload));
            }

            links.Add(Constants.Relations.Last, last)
                .Add(Constants.Relations.Metadata, LinkFormatter.StreamMetadata(operation.StreamId));

            return links;
        }
    }
}