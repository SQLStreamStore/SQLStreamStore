namespace Example
{
    using System;
    using System.Diagnostics;
    using System.Threading;
    using EasyConsole;

    internal class Program
    {
        static void Main(string[] args)
        {
            var cts = new CancellationTokenSource();
            Console.CancelKeyPress += (_, __) => cts.Cancel();

            Output.WriteLine(ConsoleColor.Yellow, "Choose a test:");
            new Menu()
                .Add(
                    "Subscribe to all",
                    () => new SubscribeToAll().Run(cts.Token))
/*                .Add(
                    "Read All",
                    () => new ReadAll().Run(cts.Token))
                .Add(
                    "Append Max Count",
                    () => new AppendMaxCount().Run(cts.Token))*/
                .Display();

            if(Debugger.IsAttached)
            {
                Console.ReadLine();
            }
        }
    }
}