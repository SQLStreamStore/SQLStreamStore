namespace SqlStreamStore.V1.Index
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
        public static IApplicationBuilder UseIndex(this IApplicationBuilder app, IndexResource index)
            => app.UseMiddlewareLogging(typeof(IndexMiddleware))
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