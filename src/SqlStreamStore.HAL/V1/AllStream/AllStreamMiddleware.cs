namespace SqlStreamStore.V1.AllStream
{
    using System.Net.Http;
    using Microsoft.AspNetCore.Builder;
    using MidFunc = System.Func<
        Microsoft.AspNetCore.Http.HttpContext,
        System.Func<System.Threading.Tasks.Task>,
        System.Threading.Tasks.Task
    >;

    internal static class AllStreamMiddleware
    {
        public static IApplicationBuilder UseAllStream(
            this IApplicationBuilder app,
            AllStreamResource allStream) => app
            .UseMiddlewareLogging(typeof(AllStreamMiddleware))
            .MapWhen(
                HttpMethod.Get,
                inner => inner.UseAccept(Constants.MediaTypes.HalJson).Use(GetStream(allStream)))
            .UseAllowedMethods(allStream);

        private static MidFunc GetStream(AllStreamResource allStream) => async (context, next) =>
        {
            var operation = new ReadAllStreamOperation(context);

            var response = await allStream.Get(operation, context.RequestAborted);

            await context.WriteResponse(response);
        };
    }
}