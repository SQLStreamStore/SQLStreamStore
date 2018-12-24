namespace LoadTests
{
    using System;
    using System.Diagnostics;
    using System.Threading;
    using EasyConsole;
    using Serilog;

    internal class Program
    {
        static void Main(string[] args)
        {
            Log.Logger = new LoggerConfiguration()
                .WriteTo
                .File("LoadTests.txt")
                .CreateLogger();

            var cts = new CancellationTokenSource();
            Console.CancelKeyPress += (_, __) => cts.Cancel();

            Output.WriteLine(ConsoleColor.Yellow, "Choose a test:");
            new Menu()
                .Add(
                    "Append with ExpectedVersion.Any",
                    async ct => await new AppendExpectedVersionAnyParallel().Run(ct))
                .Add(
                    "Read all",
                    async ct => await new ReadAll().Run(ct))
                .Add(
                    "Append max count",
                    async ct => await new AppendMaxCount().Run(ct))
                .Add(
                    "Many steam subscriptions",
                    async ct => await new StreamSubscription().Run(ct))
                .Add(
                    "Test gaps",
                    async ct => await new TestGaps().Run(ct))
                .Display(cts.Token);

            if(Debugger.IsAttached)
            {
                Console.ReadLine();
            }
        }
    }
}