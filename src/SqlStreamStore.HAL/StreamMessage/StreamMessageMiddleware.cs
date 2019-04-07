namespace SqlStreamStore.HAL.StreamMessage
{
    using System;
    using Microsoft.AspNetCore.Builder;
    using Microsoft.AspNetCore.Http;
    using Microsoft.AspNetCore.Routing;
    using SqlStreamStore.HAL.StreamMessage.MessageId;
    using SqlStreamStore.HAL.StreamMessage.Version;
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
            => builder
                .UseMiddlewareLogging(typeof(StreamMessageMiddleware))
                .UseAllowedMethods(streamMessages)
                .MapWhen(
                    context => Guid.TryParse(GetParameter(context), out _),
                    inner => inner.UseByMessageId(streamMessages))
                .MapWhen(
                    context => int.TryParse(GetParameter(context), out _),
                    inner => inner.UseByVersion(streamMessages));

        private static string GetParameter(HttpContext context) => (string) context.GetRouteValue("p");
    }
}