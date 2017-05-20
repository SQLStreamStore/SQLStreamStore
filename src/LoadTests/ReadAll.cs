﻿namespace LoadTests
{
    using System;
    using System.Diagnostics;
    using System.Threading;
    using System.Threading.Tasks;
    using EasyConsole;
    using SqlStreamStore;

    public class ReadAll : LoadTest
    {
        protected override async Task RunAsync(CancellationToken ct)
        {
            Output.WriteLine("");
            Output.WriteLine(ConsoleColor.Green, "Appends events to streams and reads them all back in a single task.");
            Output.WriteLine("");

            var streamStore = GetStore();

            await new AppendExpectedVersionAnyParallel()
                .Append(streamStore, ct);

            int readPageSize = Input.ReadInt("Read page size: ", 1, 10000);

            var stopwatch = Stopwatch.StartNew();
            int count = 0;
            var position = Position.Start;
            ReadAllPage page;
            do
            {
                page = await streamStore.ReadAllForwards(position, readPageSize, cancellationToken: ct);
                count += page.Messages.Length;
                Console.Write($"\r> Read {count}");
                position = page.NextPosition;
            } while (!page.IsEnd);

            stopwatch.Stop();
            var rate = Math.Round((decimal)count / stopwatch.ElapsedMilliseconds * 1000, 0);

            Output.WriteLine("");
            Output.WriteLine($"> {count} messages read in {stopwatch.Elapsed} ({rate} m/s)");
        }
    }
}