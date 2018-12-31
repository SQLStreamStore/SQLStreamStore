namespace SqlStreamStore.HAL.StreamBrowser
{
    using SqlStreamStore.Streams;

    internal static class StreamBrowserLinkExtensions
    {
        public static Links StreamBrowserNavigation(
            this Links links,
            ListStreamsPage listStreamsPage,
            ListStreamsOperation operation)
        {
            if(operation.ContinuationToken != null)
            {
                links.Add(
                    Constants.Relations.Next,
                    FormatLink(operation, listStreamsPage.ContinuationToken));
            }

            if(listStreamsPage.ContinuationToken != null)
            {
                links.Add(
                    Constants.Relations.Next,
                    FormatLink(operation, listStreamsPage.ContinuationToken));
            }


            return links
                .Add(
                    Constants.Relations.Browse,
                    FormatLink(operation, listStreamsPage.ContinuationToken))
                .Self();
        }

        private static string FormatLink(ListStreamsOperation operation, string continuationToken) =>
            $"streams?p={operation.Pattern.Value}&t={operation.PatternType}&c={continuationToken}&m={operation.MaxCount}";
    }
}