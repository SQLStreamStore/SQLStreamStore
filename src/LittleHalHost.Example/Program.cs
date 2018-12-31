using System;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.Extensions.DependencyInjection;
using Newtonsoft.Json;
using Serilog;
using SqlStreamStore;
using SqlStreamStore.HAL;
using SqlStreamStore.Streams;
using MidFunc = System.Func<
    Microsoft.AspNetCore.Http.HttpContext,
    System.Func<System.Threading.Tasks.Task>,
    System.Threading.Tasks.Task
>;

namespace Example
{
    class Program
    {
        public static async Task Main(string[] args)
        {
            Log.Logger = new LoggerConfiguration()
                .MinimumLevel.Debug()
                .Enrich.FromLogContext()
                .WriteTo.Console()
                .CreateLogger();

            using(var streamStore = new InMemoryStreamStore())
            using(var host = new WebHostBuilder()
                .UseKestrel()
                .UseStartup(new Startup(streamStore, new SqlStreamStoreMiddlewareOptions
                {
                    UseCanonicalUrls = false
                }))
                .UseSerilog()
                .UseUrls("http://localhost:5050")
                .Build())
            {
                // Simulating some messages in the store here (only one stream)
                var random = new Random();
                var id = new Guid("cbf68be34d9547eb9b4a390fd2aa417b");
                var messages = new NewStreamMessage[random.Next(1000, 2000)];
                for(var index = 0; index < messages.Length; index++)
                {
                    messages[index] = new NewStreamMessage(
                        Guid.NewGuid(),
                        "ItineraryPublished",
                        JsonConvert.SerializeObject(new {
                            ItineraryId = id,
                            Data = index
                        })
                    );
                }
                await streamStore.AppendToStream(id.ToString("N"), ExpectedVersion.NoStream, messages);

                // bootstrapping the server
                var source = new CancellationTokenSource();
                var serverTask = host.RunAsync(source.Token);
                // press enter to exit
                Console.WriteLine("Running ...");
                Console.ReadLine();
                source.Cancel();

                await serverTask;
            }
        }
    }

    internal static class WebHostBuilderExtensions
    {
        public static IWebHostBuilder UseStartup(this IWebHostBuilder builder, IStartup startup)
            => builder
                .ConfigureServices(services => services.AddSingleton(startup))
                .UseSetting(WebHostDefaults.ApplicationKey, startup.GetType().AssemblyQualifiedName);
    }

    internal class Startup : IStartup
    {
        private readonly IStreamStore _streamStore;
        private readonly SqlStreamStoreMiddlewareOptions _options;

        public Startup(
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
            .Use(CatchAndDisplayErrors)
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
    }
}