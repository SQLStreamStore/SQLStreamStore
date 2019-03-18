namespace SqlStreamStore.HAL.DevServer
{
    using System;
    using System.Linq;
    using System.Threading;
    using System.Threading.Tasks;
    using Microsoft.AspNetCore.Hosting;
    using Microsoft.Extensions.Configuration;
    using Serilog;
    using SqlStreamStore.Streams;

    internal class Program : IDisposable
    {
        private static readonly Random s_random = new Random();
        private readonly CancellationTokenSource _cts;
        private readonly IConfigurationRoot _configuration;

        private bool Interactive => _configuration.GetValue<bool>("interactive");
        private bool UseCanonicalUrls => _configuration.GetValue<bool>("canonical");

        public static async Task<int> Main(string[] args)
        {
            Log.Logger = new LoggerConfiguration()
                .MinimumLevel.Debug()
                .Enrich.FromLogContext()
                .WriteTo.Console()
                .CreateLogger();

            using(var program = new Program(args))
            {
                return await program.Run();
            }
        }

        private Program(string[] args)
        {
            _cts = new CancellationTokenSource();
            _configuration = new ConfigurationBuilder()
                .AddEnvironmentVariables()
                .AddCommandLine(args)
                .Build();
        }

        private async Task<int> Run()
        {
            try
            {
                using(var streamStore = await SqlStreamStoreFactory.Create())
                using(var host = new WebHostBuilder()
                    .UseKestrel()
                    .UseStartup(new DevServerStartup(streamStore, new SqlStreamStoreMiddlewareOptions
                    {
                        UseCanonicalUrls = UseCanonicalUrls,
                        ServerAssembly = typeof(Program).Assembly
                    }))
                    .UseSerilog()
                    .Build())
                {
                    var serverTask = host.RunAsync(_cts.Token);

                    if(Interactive)
                    {
                        Log.Warning("Running interactively.");
                        DisplayMenu(streamStore);
                    }

                    await serverTask;

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

        private static void DisplayMenu(IStreamStore streamStore)
        {
            while(true)
            {
                Console.WriteLine("Press w to write 10 messages each to 100 streams");
                Console.WriteLine("Press t to write 100 messages each to 10 streams");
                Console.WriteLine("Press ESC to exit");

                var key = Console.ReadKey();

                switch(key.Key)
                {
                    case ConsoleKey.Escape:
                        return;
                    case ConsoleKey.W:
                        Write(streamStore, 10, 100);
                        break;
                    case ConsoleKey.T:
                        Write(streamStore, 100, 10);
                        break;
                    default:
                        Console.WriteLine("Computer says no");
                        break;
                }
            }
        }

        private static void Write(IStreamStore streamStore, int messageCount, int streamCount)
        {
            var streams = Enumerable.Range(0, streamCount).Select(_ => $"test-{Guid.NewGuid():n}").ToList();

            Task.Run(() => Task.WhenAll(
                from streamId in streams
                select streamStore.AppendToStream(
                    streamId,
                    ExpectedVersion.NoStream,
                    GenerateMessages(messageCount))));
        }

        private static NewStreamMessage[] GenerateMessages(int messageCount)
            => Enumerable.Range(0, messageCount)
                .Select(_ => new NewStreamMessage(
                    Guid.NewGuid(),
                    "test",
                    $@"{{ ""foo"": ""{Guid.NewGuid()}"", ""baz"": {{  }}, ""qux"": [ {
                            string.Join(", ",
                                Enumerable
                                    .Range(0, messageCount).Select(max => s_random.Next(max)))
                        } ] }}",
                    "{}"))
                .ToArray();

        public void Dispose()
        {
            _cts?.Dispose();
        }
    }
}