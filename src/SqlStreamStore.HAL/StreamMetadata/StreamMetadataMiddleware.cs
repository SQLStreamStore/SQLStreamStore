namespace SqlStreamStore.HAL.StreamMetadata
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

    internal static class StreamMetadataMiddleware
    {
        public static IApplicationBuilder UseStreamMetadata(
            this IApplicationBuilder builder,
            StreamMetadataResource streamMetadata)
            => builder.MapWhen(IsMatch, Configure(streamMetadata));

        private static bool IsMatch(HttpContext context)
            => context.Request.Path.IsStreamMetadata();

        private static bool IsStreamMetadata(this PathString requestPath)
        {
            if(!requestPath.StartsWithSegments(Constants.Streams.StreamsPath))
            {
                return false;
            }

            var segments = requestPath.Value?.Split('/');

            return segments?.Length == 4 && segments[3] == Constants.Streams.Metadata;
        }

        private static Action<IApplicationBuilder> Configure(StreamMetadataResource streamsMetadata)
            => builder => builder
                .UseMiddlewareLogging(typeof(StreamMetadataMiddleware))
                .MapWhen(HttpMethod.Post, inner => inner.UseAccept(Constants.MediaTypes.HalJson).Use(SetStreamMetadata(streamsMetadata)))
                .MapWhen(HttpMethod.Get, inner => inner.UseAccept(Constants.MediaTypes.HalJson).Use(GetStreamMetadata(streamsMetadata)))
                .UseAllowedMethods(streamsMetadata);

        private static MidFunc SetStreamMetadata(StreamMetadataResource streamsMetadata)
            => async (context, next) =>
            {
                var operation = await SetStreamMetadataOperation.Create(context.Request, context.RequestAborted);

                var response = await streamsMetadata.Post(operation, context.RequestAborted);

                await context.WriteResponse(response);
            };

        private static MidFunc GetStreamMetadata(StreamMetadataResource streamsMetadata)
            => async (context, next) =>
            {
                var operation = new GetStreamMetadataOperation(context.Request);

                var response = await streamsMetadata.Get(operation, context.RequestAborted);

                await context.WriteResponse(response);
            };
    }
}