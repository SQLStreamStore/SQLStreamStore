namespace SqlStreamStore.HAL.Index
{
    using System;
    using System.Net.Http;
    using System.Threading.Tasks;
    using Microsoft.AspNetCore.Builder;
    using Microsoft.AspNetCore.Http;
    using MidFunc = System.Func<
        Microsoft.AspNetCore.Http.HttpContext,
        System.Func<System.Threading.Tasks.Task>,
        System.Threading.Tasks.Task
    >;

    internal static class IndexMiddleware
    {
        public static IApplicationBuilder UseIndex(this IApplicationBuilder builder, IndexResource index)
            => builder.MapWhen(
                IsMatch,
                Configure(index));

        private static bool IsMatch(HttpContext context)
            => context.Request.Path == Constants.Streams.IndexPath;

        private static Action<IApplicationBuilder> Configure(IndexResource index)
            => builder => builder
                .UseMiddlewareLogging(typeof(IndexMiddleware))
                .MapWhen(HttpMethod.Get, inner => inner.UseAccept(Constants.MediaTypes.HalJson).Use(Index(index)))
                .UseAllowedMethods(index);

        private static MidFunc Index(IndexResource index)
        {
            var response = index.Get();

            Task Index(HttpContext context, Func<Task> next) => context.WriteResponse(response);

            return Index;
        }
    }
}