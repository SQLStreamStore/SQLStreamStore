namespace SqlStreamStore.HAL.ApplicationServer.Browser
{
    using System;
    using System.Linq;
    using System.Net.Http.Headers;
    using System.Threading.Tasks;
    using Microsoft.AspNetCore.Builder;
    using Microsoft.AspNetCore.Http;
    using Microsoft.Extensions.FileProviders;
    using Serilog;
    using MidFunc = System.Func<
        Microsoft.AspNetCore.Http.HttpContext,
        System.Func<System.Threading.Tasks.Task>,
        System.Threading.Tasks.Task
    >;

    internal static class SqlStreamStoreBrowserMiddleware
    {
        public static IApplicationBuilder UseSqlStreamStoreBrowser(
            this IApplicationBuilder builder)
        {
            var sqlStreamStoreBrowserFileProvider = new EmbeddedFileProvider(
                typeof(SqlStreamStoreBrowserMiddleware).Assembly,
                typeof(SqlStreamStoreBrowserMiddleware).Namespace);

            var staticFiles = typeof(SqlStreamStoreBrowserMiddleware).Assembly.GetManifestResourceNames()
                .Where(name => name.StartsWith(typeof(SqlStreamStoreBrowserMiddleware).Namespace));

            Log.Debug(
                $"The following embedded resources were found and will be served as  static content: {string.Join(", ", staticFiles)}");

            return builder.Use(IndexPage).UseStaticFiles(new StaticFileOptions
            {
                FileProvider = sqlStreamStoreBrowserFileProvider
            });

            Task IndexPage(HttpContext context, Func<Task> next)
            {
                if(GetAcceptHeaders(context.Request).Contains("text/html"))
                {
                    context.Request.Path = new PathString("/index.html");
                }

                return next();
            }
        }

        private static string[] GetAcceptHeaders(HttpRequest contextRequest)
            => Array.ConvertAll(
                contextRequest.Headers.GetCommaSeparatedValues("Accept"),
                value => MediaTypeWithQualityHeaderValue.TryParse(value, out var header)
                    ? header.MediaType
                    : null);
    }
}