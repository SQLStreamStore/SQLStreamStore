namespace SqlStreamStore.HAL.AllStreamMessage
{
    using System.Net.Http;
    using Microsoft.AspNetCore.Builder;
    using MidFunc = System.Func<
        Microsoft.AspNetCore.Http.HttpContext,
        System.Func<System.Threading.Tasks.Task>,
        System.Threading.Tasks.Task
    >;

    internal static class AllStreamMessageMiddleware
    {
        public static IApplicationBuilder UseAllStreamMessage(
            this IApplicationBuilder builder,
            AllStreamMessageResource allStreamMessages)
            => builder
                .UseMiddlewareLogging(typeof(AllStreamMessageMiddleware))
                .MapWhen(
                    HttpMethod.Get,
                    inner => inner.UseAccept(Constants.MediaTypes.HalJson).Use(GetStreamMessage(allStreamMessages)))
                .UseAllowedMethods(allStreamMessages);

        private static MidFunc GetStreamMessage(AllStreamMessageResource allStreamMessages) => async (context, next) =>
        {
            var response = await allStreamMessages.Get(
                new ReadAllStreamMessageOperation(context),
                context.RequestAborted);

            await context.WriteResponse(response);
        };
    }
}