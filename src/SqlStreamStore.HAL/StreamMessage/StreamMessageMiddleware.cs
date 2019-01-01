namespace SqlStreamStore.HAL.StreamMessage
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

    internal static class StreamMessageMiddleware
    {
        public static IApplicationBuilder UseStreamMessages(
            this IApplicationBuilder builder,
            StreamMessageResource streamMessages)
            => builder.MapWhen(IsMatch, Configure(streamMessages));

        private static bool IsMatch(HttpContext context)
            => context.Request.Path.IsStreamMessage();

        public static bool IsStreamMessage(this PathString requestPath)
        {
            if(!requestPath.StartsWithSegments(Constants.Streams.StreamsPath))
            {
                return false;
            }

            var segments = requestPath.Value?.Split('/');

            return segments?.Length == 4
                   && (int.TryParse(segments[3], out _)
                       || Guid.TryParse(segments[3], out _));
        }

        private static Action<IApplicationBuilder> Configure(StreamMessageResource streamMessages)
            => builder => builder
                .UseMiddlewareLogging(typeof(StreamMessageMiddleware))
                .MapWhen(HttpMethod.Get, inner => inner.UseAccept(Constants.MediaTypes.HalJson).Use(GetStreamMessage(streamMessages)))
                .MapWhen(HttpMethod.Delete, inner => inner.Use(DeleteStreamMessage(streamMessages)))
                .UseAllowedMethods(streamMessages);


        private static MidFunc GetStreamMessage(StreamMessageResource streamMessages) => async (context, next) =>
        {
            var operation = new ReadStreamMessageByStreamVersionOperation(context.Request);

            var response = await streamMessages.Get(operation, context.RequestAborted);

            await context.WriteResponse(response);
        };

        private static MidFunc DeleteStreamMessage(StreamMessageResource streamMessages) => async (context, next) =>
        {
            var operation = new DeleteStreamMessageOperation(context.Request);

            var response = await streamMessages.Delete(operation, context.RequestAborted);

            await context.WriteResponse(response);
        };
    }
}