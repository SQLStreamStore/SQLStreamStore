namespace SqlStreamStore.Docs
{
    using System;
    using System.Net.Http;
    using System.Threading.Tasks;
    using Microsoft.AspNetCore.Builder;
    using Microsoft.AspNetCore.Http;
    using Microsoft.AspNetCore.Routing;
    using MidFunc = System.Func<
        Microsoft.AspNetCore.Http.HttpContext,
        System.Func<System.Threading.Tasks.Task>,
        System.Threading.Tasks.Task
    >;

    internal static class DocsMiddleware
    {
        public static IApplicationBuilder UseDocs(this IApplicationBuilder app, DocsResource documentation) => app
            .UseMiddlewareLogging(typeof(DocsMiddleware))
            .MapWhen(
                HttpMethod.Get,
                inner => inner.UseAccept(
                        Constants.MediaTypes.TextMarkdown,
                        Constants.MediaTypes.JsonHyperSchema)
                    .Use(Docs(documentation)))
            .UseAllowedMethods(documentation);

        private static MidFunc Docs(DocsResource documentation)
        {
            Task Docs(HttpContext context, Func<Task> next)
            {
                bool TryGetDocs(out Response value) =>
                    (value = documentation.Get(
                        context.GetRouteData().GetDoc(),
                        context.Request.Headers["Accept"])) != null;

                return !TryGetDocs(out var response)
                    ? next()
                    : context.WriteResponse(response);
            }

            return Docs;
        }
    }
}