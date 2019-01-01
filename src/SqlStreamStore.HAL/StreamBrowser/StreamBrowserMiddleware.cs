namespace SqlStreamStore.HAL.StreamBrowser
{
    using System;
    using System.Net.Http;
    using Microsoft.AspNetCore.Builder;
    using Microsoft.AspNetCore.Http;
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
            => builder.MapWhen(IsMatch, Configure(streamBrowser));
        
        private static bool IsMatch(HttpContext context)
            => context.Request.Path.IsStreamBrowser();

        public static bool IsStreamBrowser(this PathString requestPath)
            => requestPath == Constants.Streams.StreamBrowserPath;

        private static Action<IApplicationBuilder> Configure(StreamBrowserResource streamBrowser)
            => builder => builder
                .UseMiddlewareLogging(typeof(StreamBrowserMiddleware))
                .MapWhen(HttpMethod.Get, inner => inner.Use(BrowseStreams(streamBrowser)));

        private static MidFunc BrowseStreams(StreamBrowserResource streamBrowser)
            => async (context, next) =>
            {
                var response = await streamBrowser.Get(
                    new ListStreamsOperation(context.Request),
                    context.RequestAborted);

                await context.WriteResponse(response);
            };
    }
}