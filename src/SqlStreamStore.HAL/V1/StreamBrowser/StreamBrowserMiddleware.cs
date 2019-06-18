namespace SqlStreamStore.V1.StreamBrowser
{
    using System.Net.Http;
    using Microsoft.AspNetCore.Builder;
    using MidFunc = System.Func<
        Microsoft.AspNetCore.Http.HttpContext,
        System.Func<System.Threading.Tasks.Task>,
        System.Threading.Tasks.Task
    >;

    internal static class StreamBrowserMiddleware
    {
        public static IApplicationBuilder UseStreamBrowser(
            this IApplicationBuilder builder,
            StreamBrowserResource streamBrowser)
            => builder
                .UseMiddlewareLogging(typeof(StreamBrowserMiddleware))
                .MapWhen(HttpMethod.Get, inner => inner.Use(BrowseStreams(streamBrowser)));

        private static MidFunc BrowseStreams(StreamBrowserResource streamBrowser)
            => async (context, next) =>
            {
                var response = await streamBrowser.Get(
                    new ListStreamsOperation(context),
                    context.RequestAborted);

                await context.WriteResponse(response);
            };
    }
}