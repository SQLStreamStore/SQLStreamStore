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
                    LinkFormatter.ListStreams(operation.Pattern, operation.MaxCount, operation.ContinuationToken));
            }

            if(listStreamsPage.ContinuationToken != null)
            {
                links.Add(
                    Constants.Relations.Next,
                    LinkFormatter.ListStreams(operation.Pattern, operation.MaxCount, operation.ContinuationToken));
            }


            return links
                .Add(
                    Constants.Relations.Browse,
                    LinkFormatter.ListStreams(operation.Pattern, operation.MaxCount, operation.ContinuationToken))
                .Self();
        }
    }
}