namespace SqlStreamStore.HAL.AllStream
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

    internal static class AllStreamMiddleware
    {
        public static IApplicationBuilder UseAllStream(
            this IApplicationBuilder builder,
            AllStreamResource allStream)
            => builder.MapWhen(IsMatch, Configure(allStream));

        private static bool IsMatch(HttpContext context)
            => context.Request.Path.IsAllStream();

        private static bool IsAllStream(this PathString pathString)
            => pathString.StartsWithSegments(Constants.Streams.AllStreamPath)
               && pathString.Value.Split('/').Length == 2;

        private static Action<IApplicationBuilder> Configure(AllStreamResource allStream)
            => builder => builder
                .UseMiddlewareLogging(typeof(AllStreamMiddleware))
                .MapWhen(
                    HttpMethod.Get,
                    inner => inner.UseAccept(Constants.MediaTypes.HalJson).Use(GetStream(allStream)))
                .UseAllowedMethods(allStream);

        private static MidFunc GetStream(AllStreamResource allStream) => async (context, next) =>
        {
            var operation = new ReadAllStreamOperation(context.Request);

            var response = await allStream.Get(operation, context.RequestAborted);

            await context.WriteResponse(response);
        };
    }
}