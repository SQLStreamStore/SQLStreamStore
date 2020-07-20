namespace LoadTests
{
    using System;
    using System.Diagnostics;
    using System.Threading;
    using System.Threading.Tasks;
    using EasyConsole;
    using Serilog;

    internal class Program
    {
        static async Task Main(string[] args)
        {
            Log.Logger = new LoggerConfiguration()
                .WriteTo
                .File("LoadTests.txt")
                .CreateLogger();

            var cts = new CancellationTokenSource();
            Console.CancelKeyPress += (_, __) => cts.Cancel();

            Output.WriteLine(ConsoleColor.Yellow, "Choose a test:");
            await new Menu()
                .Add(
                    "Append with ExpectedVersion.Any",
                    async ct => await new AppendExpectedVersionAnyParallel().Run(ct))
                .Add(
                    "Append with ExpectedVersion.NoStream",
                    async ct => await new AppendExpectedVersionNoStreamParallel().Run(ct))
                .Add(
                    "Read all forwards",
                    async ct => await new ReadAllForwards().Run(ct))
                .Add(
                    "Read all backwards",
                    async ct => await new ReadAllBackwards().Run(ct))
                .Add(
                    "Append max count",
                    async ct => await new AppendMaxCount().Run(ct))
                .Add(
                    "Many steam subscriptions",
                    async ct => await new StreamSubscription().Run(ct))
                .Add(
                    "Test gaps",
                    async ct => await new TestGaps().Run(ct))
                .Add(
                    "Append Read Deadlocks",
                    async ct => await new AppendsReadsDeadlocks().Run(ct))
                .Display(cts.Token);

            if(Debugger.IsAttached)
            {
                Console.ReadLine();
            }
        }
    }
}
