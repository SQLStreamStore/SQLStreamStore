namespace SqlStreamStore.HAL.Streams
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

    internal static class StreamsMiddleware
    {
        public static IApplicationBuilder UseStreams(this IApplicationBuilder builder, StreamResource streams)
            => builder.MapWhen(IsMatch, Configure(streams));

        private static bool IsMatch(HttpContext context)
            => context.Request.Path.IsStreams();

        private static bool IsStreams(this PathString requestPath)
            => requestPath.StartsWithSegments(Constants.Streams.StreamsPath)
               && requestPath.Value?.Split('/').Length == 3;

        private static Action<IApplicationBuilder> Configure(StreamResource streams)
            => builder => builder
                .UseMiddlewareLogging(typeof(StreamsMiddleware))
                .MapWhen(HttpMethod.Get, inner => inner.UseAccept(Constants.MediaTypes.HalJson).Use(GetStream(streams)))
                .MapWhen(HttpMethod.Delete, inner => inner.Use(DeleteStream(streams)))
                .MapWhen(
                    HttpMethod.Post,
                    inner => inner.UseAccept(Constants.MediaTypes.HalJson).Use(AppendStream(streams)))
                .UseAllowedMethods(streams);

        private static MidFunc GetStream(StreamResource streams) => async (context, next) =>
        {
            var operation = new ReadStreamOperation(context.Request);

            var response = await streams.Get(operation, context.RequestAborted);

            await context.WriteResponse(response);
        };

        private static MidFunc DeleteStream(StreamResource stream) =>
            async (context, next) =>
            {
                var operation = new DeleteStreamOperation(context.Request);

                var response = await stream.Delete(operation, context.RequestAborted);

                await context.WriteResponse(response);
            };

        private static MidFunc AppendStream(StreamResource stream) =>
            async (context, next) =>
            {
                var operation = await AppendStreamOperation.Create(context.Request, context.RequestAborted);

                var response = await stream.Post(operation, context.RequestAborted);

                await context.WriteResponse(response);
            };
    }
}