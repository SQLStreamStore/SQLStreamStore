namespace SqlStreamStore.HAL.StreamMessage.MessageId
{
    using System.Net.Http;
    using Microsoft.AspNetCore.Builder;
    using MidFunc = System.Func<
        Microsoft.AspNetCore.Http.HttpContext,
        System.Func<System.Threading.Tasks.Task>,
        System.Threading.Tasks.Task
    >;

    internal static class StreamMessageByMessageIdMiddleware
    {
        public static IApplicationBuilder UseByMessageId(
            this IApplicationBuilder builder,
            StreamMessageResource streamMessages)
            => builder
                .UseMiddlewareLogging(typeof(StreamMessageByMessageIdMiddleware))
                .MapWhen(HttpMethod.Delete, inner => inner.Use(DeleteStreamMessage(streamMessages)))
                .UseAllowedMethods(streamMessages);

        private static MidFunc DeleteStreamMessage(StreamMessageResource streamMessages) => async (context, next) =>
        {
            var operation = new DeleteStreamMessageByMessageIdOperation(context);

            var response = await streamMessages.Delete(operation, context.RequestAborted);

            await context.WriteResponse(response);
        };
    }
}