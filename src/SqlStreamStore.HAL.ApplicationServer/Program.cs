namespace SqlStreamStore.HAL.ApplicationServer
{
    using System;
    using System.Threading;
    using System.Threading.Tasks;
    using Microsoft.AspNetCore.Hosting;
    using Serilog;

    internal class Program : IDisposable
    {
        private readonly CancellationTokenSource _cts;
        private readonly SqlStreamStoreHalConfiguration _configuration;
        private readonly SqlStreamStoreFactory _factory;

        public static async Task<int> Main(string[] args)
        {
            using(var program = new Program(args))
            {
                return await program.Run();
            }
        }

        private Program(string[] args)
        {
            _configuration = new SqlStreamStoreHalConfiguration(Environment.GetEnvironmentVariables(), args);

            Log.Logger = new LoggerConfiguration()
                .MinimumLevel.Is(_configuration.LogLevel)
                .Enrich.FromLogContext()
                .WriteTo.Console()
                .CreateLogger();

            _cts = new CancellationTokenSource();
            _factory = new SqlStreamStoreFactory(_configuration);
        }

        private async Task<int> Run()
        {
            try
            {
                using(var streamStore = await _factory.Create(_cts.Token))
                using(var host = new WebHostBuilder()
                    .UseKestrel()
                    .UseStartup(new ApplicationServerStartup(streamStore,
                        new SqlStreamStoreMiddlewareOptions
                        {
                            UseCanonicalUrls = _configuration.UseCanonicalUris
                        }))
                    .UseSerilog()
                    .Build())
                {
                    await Task.WhenAll(
                        host.RunAsync(_cts.Token),
                        host.WaitForShutdownAsync(_cts.Token));

                    return 0;
                }
            }
            catch(Exception ex)
            {
                Log.Fatal(ex, "Host terminated unexpectedly.");
                return 1;
            }
            finally
            {
                Log.CloseAndFlush();
            }
        }

        public void Dispose()
        {
            _cts?.Dispose();
        }
    }
}