namespace SqlStreamStore.V1.AllStream
{
    using System.Linq;
    using SqlStreamStore.V1.Streams;

    internal static class AllStreamLinkExtensions
    {
        public static Links AllStreamNavigation(
            this Links links,
            ReadAllPage page,
            ReadAllStreamOperation operation)
        {
            var first = LinkFormatter.ReadAllForwards(
                Position.Start,
                operation.MaxCount,
                operation.EmbedPayload);

            var last = LinkFormatter.ReadAllBackwards(
                Position.End,
                operation.MaxCount,
                operation.EmbedPayload);

            links.Add(Constants.Relations.First, first);

            if(operation.Self != first && !page.IsEnd)
            {
                links.Add(
                    Constants.Relations.Previous,
                    LinkFormatter.ReadAllBackwards(
                        page.Messages.Min(m => m.Position) - 1,
                        operation.MaxCount,
                        operation.EmbedPayload));
            }

            links.Add(Constants.Relations.Feed, operation.Self).Self();

            if(operation.Self != last && !page.IsEnd)
            {
                links.Add(
                    Constants.Relations.Next,
                    LinkFormatter.ReadAllForwards(
                        page.Messages.Max(m => m.Position) + 1,
                        operation.MaxCount,
                        operation.EmbedPayload));
            }

            links.Add(Constants.Relations.Last, last);

            return links;
        }
    }
}