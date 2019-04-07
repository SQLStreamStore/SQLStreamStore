namespace SqlStreamStore.HAL.StreamMetadata
{
    using System.Net.Http;
    using Microsoft.AspNetCore.Builder;
    using MidFunc = System.Func<
        Microsoft.AspNetCore.Http.HttpContext,
        System.Func<System.Threading.Tasks.Task>,
        System.Threading.Tasks.Task
    >;

    internal static class StreamMetadataMiddleware
    {
        public static IApplicationBuilder UseStreamMetadata(
            this IApplicationBuilder builder,
            StreamMetadataResource streamMetadata)
            => builder
                .UseMiddlewareLogging(typeof(StreamMetadataMiddleware))
                .MapWhen(HttpMethod.Post, inner => inner.UseAccept(Constants.MediaTypes.HalJson).Use(SetStreamMetadata(streamMetadata)))
                .MapWhen(HttpMethod.Get, inner => inner.UseAccept(Constants.MediaTypes.HalJson).Use(GetStreamMetadata(streamMetadata)))
                .UseAllowedMethods(streamMetadata);

        private static MidFunc SetStreamMetadata(StreamMetadataResource streamsMetadata)
            => async (context, next) =>
            {
                var operation = await SetStreamMetadataOperation.Create(context);

                var response = await streamsMetadata.Post(operation, context.RequestAborted);

                await context.WriteResponse(response);
            };

        private static MidFunc GetStreamMetadata(StreamMetadataResource streamsMetadata)
            => async (context, next) =>
            {
                var operation = new GetStreamMetadataOperation(context);

                var response = await streamsMetadata.Get(operation, context.RequestAborted);

                await context.WriteResponse(response);
            };
    }
}