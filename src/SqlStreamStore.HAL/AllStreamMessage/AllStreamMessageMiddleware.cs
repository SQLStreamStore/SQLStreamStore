namespace SqlStreamStore.HAL.AllStreamMessage
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

    internal static class AllStreamMessageMiddleware
    {
        public static IApplicationBuilder UseAllStreamMessage(
            this IApplicationBuilder builder,
            AllStreamMessageResource allStreamMessageResource)
            => builder.MapWhen(IsMatch, Configure(allStreamMessageResource));

        private static bool IsMatch(HttpContext context)
            => context.Request.Path.IsAllStreamMessage();

        private static bool IsAllStreamMessage(this PathString requestPath)
            => requestPath.StartsWithSegments(Constants.Streams.AllStreamPath)
               && long.TryParse(requestPath.Value?.Remove(0, 2 + Constants.Streams.All.Length), out _);

        private static Action<IApplicationBuilder> Configure(AllStreamMessageResource allStreamMessages)
            => builder => builder
                .UseMiddlewareLogging(typeof(AllStreamMessageMiddleware))
                .MapWhen(
                    HttpMethod.Get,
                    inner => inner.UseAccept(Constants.MediaTypes.HalJson).Use(GetStreamMessage(allStreamMessages)))
                .UseAllowedMethods(allStreamMessages);

        private static MidFunc GetStreamMessage(AllStreamMessageResource allStreamMessages) => async (context, next) =>
        {
            var response = await allStreamMessages.Get(
                new ReadAllStreamMessageOperation(context.Request),
                context.RequestAborted);

            await context.WriteResponse(response);
        };
    }
}