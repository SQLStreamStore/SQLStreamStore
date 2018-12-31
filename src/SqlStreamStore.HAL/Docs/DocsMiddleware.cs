namespace SqlStreamStore.HAL.Docs
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

    internal static class DocsMiddleware
    {
        public static IApplicationBuilder UseDocs(
            this IApplicationBuilder builder,
            DocsResource documentation) 
            => builder.MapWhen(IsMatch, Configure(documentation));
        
        private static bool IsMatch(HttpContext context)
            => context.Request.Path.IsDocs();

        private static bool IsDocs(this PathString requestPath)
            => requestPath.StartsWithSegments("/docs");

        private static Action<IApplicationBuilder> Configure(DocsResource documentation)
        {
            Task Docs(HttpContext context, Func<Task> next)
            {
                Response response;
                
                return !context.Request.Path.StartsWithSegments("/docs", out var rel)
                       || (response = documentation.Get(rel.Value.Remove(0, 1))) == null
                    ? next()
                    : context.WriteResponse(response);
            }

            return builder => builder
                .UseMiddlewareLogging(typeof(DocsMiddleware))
                .MapWhen(HttpMethod.Get, inner => inner.UseAccept(Constants.MediaTypes.TextMarkdown).Use(Docs))
                .UseAllowedMethods(documentation);
        }
    }
}