namespace SqlStreamStore.HAL.StreamMessage.Version
{
    using System.Net.Http;
    using Microsoft.AspNetCore.Builder;
    using MidFunc = System.Func<
        Microsoft.AspNetCore.Http.HttpContext,
        System.Func<System.Threading.Tasks.Task>,
        System.Threading.Tasks.Task
    >;

    internal static class StreamMessageByVersionMiddleware
    {
        public static IApplicationBuilder UseByVersion(
            this IApplicationBuilder builder,
            StreamMessageResource streamMessages)
            => builder
                .UseMiddlewareLogging(typeof(StreamMessageByVersionMiddleware))
                .MapWhen(HttpMethod.Get,
                    inner => inner.UseAccept(Constants.MediaTypes.HalJson).Use(GetStreamMessage(streamMessages)))
                .MapWhen(HttpMethod.Delete, inner => inner.Use(DeleteStreamMessage(streamMessages)))
                .UseAllowedMethods(streamMessages);


        private static MidFunc GetStreamMessage(StreamMessageResource streamMessages) => async (context, next) =>
        {
            var operation = new ReadStreamMessageByStreamVersionOperation(context);

            var response = await streamMessages.Get(operation, context.RequestAborted);

            await context.WriteResponse(response);
        };

        private static MidFunc DeleteStreamMessage(StreamMessageResource streamMessages) => async (context, next) =>
        {
            var operation = new DeleteStreamMessageByVersionOperation(context);

            var response = await streamMessages.Delete(operation, context.RequestAborted);

            await context.WriteResponse(response);
        };
    }
}