namespace SqlStreamStore.HAL.ApplicationServer
{
    using System;
    using System.Threading.Tasks;
    using Microsoft.AspNetCore.Builder;
    using Microsoft.AspNetCore.Hosting;
    using Microsoft.AspNetCore.Http;
    using Microsoft.Extensions.DependencyInjection;
    using Serilog;
    using SqlStreamStore.HAL.ApplicationServer.Browser;
    using MidFunc = System.Func<
        Microsoft.AspNetCore.Http.HttpContext,
        System.Func<System.Threading.Tasks.Task>,
        System.Threading.Tasks.Task
    >;

    internal class ApplicationServerStartup : IStartup
    {
        private readonly IStreamStore _streamStore;
        private readonly SqlStreamStoreMiddlewareOptions _options;

        public ApplicationServerStartup(
            IStreamStore streamStore,
            SqlStreamStoreMiddlewareOptions options)
        {
            _streamStore = streamStore;
            _options = options;
        }

        public IServiceProvider ConfigureServices(IServiceCollection services) => services
            .AddResponseCompression(options => options.MimeTypes = new[] { "application/hal+json" })
            .BuildServiceProvider();

        public void Configure(IApplicationBuilder app) => app
            .UseResponseCompression()
            .Use(VaryAccept)
            .Use(CatchAndDisplayErrors)
            .UseSqlStreamStoreBrowser()
            .UseSqlStreamStoreHal(_streamStore, _options);

        private static MidFunc CatchAndDisplayErrors => async (context, next) =>
        {
            try
            {
                await next();
            }
            catch(Exception ex)
            {
                Log.Warning(ex, "Error during request.");
            }
        };

        private static MidFunc VaryAccept => (context, next) =>
        {
            Task Vary()
            {
                context.Response.Headers.AppendCommaSeparatedValues("Vary", "Accept");

                return Task.CompletedTask;
            }

            context.Response.OnStarting(Vary);

            return next();
        };
    }
}