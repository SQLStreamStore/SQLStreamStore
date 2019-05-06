namespace SqlStreamStore.HAL.Streams
{
    using System.Linq;
    using SqlStreamStore.Streams;

    internal static class StreamsLinkExtensions
    {
        public static Links StreamsNavigation(this Links links, ReadStreamPage page, ReadStreamOperation operation)
        {
            var baseAddress = $"{Constants.Paths.Streams}/{operation.StreamId}";

            var first = Links.FormatForwardLink(
                baseAddress,
                operation.MaxCount,
                StreamVersion.Start,
                operation.EmbedPayload);

            var last = Links.FormatBackwardLink(
                baseAddress,
                operation.MaxCount,
                StreamVersion.End,
                operation.EmbedPayload);

            links.Add(Constants.Relations.First, first);

            if(operation.Self != first && !page.IsEnd)
            {
                links.Add(
                    Constants.Relations.Previous,
                    Links.FormatBackwardLink(
                        baseAddress,
                        operation.MaxCount,
                        page.Messages.Min(m => m.StreamVersion) - 1,
                        operation.EmbedPayload));
            }

            links.Add(Constants.Relations.Feed, operation.Self, operation.StreamId).Self();

            if(operation.Self != last && !page.IsEnd)
            {
                links.Add(
                    Constants.Relations.Next,
                    Links.FormatForwardLink(
                        baseAddress,
                        operation.MaxCount,
                        page.Messages.Max(m => m.StreamVersion) + 1,
                        operation.EmbedPayload));
            }

            links.Add(Constants.Relations.Last, last)
                .Add(Constants.Relations.Metadata,
                    $"{baseAddress}/metadata");

            return links;
        }
    }
}