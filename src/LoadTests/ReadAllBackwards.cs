namespace LoadTests
{
    using System;
    using System.Diagnostics;
    using System.Threading;
    using System.Threading.Tasks;
    using EasyConsole;
    using SqlStreamStore.Streams;

    public class ReadAllBackwards : LoadTest
    {
        public override async Task Run(CancellationToken ct)
        {
            Output.WriteLine("");
            Output.WriteLine(ConsoleColor.Green, "Appends events to streams and reads them all back in a single task.");
            Output.WriteLine("");

            var (streamStore, dispose, _) = await GetStore(ct);

            try
            {
                await new UniqueStreams()
                    .Append(streamStore, ct);

                int readPageSize = Input.ReadInt("Read page size: ", 1, 10000);
                var prefetch = await Input.ReadEnum<YesNo>("Prefetch: ", ct);

                var stopwatch = Stopwatch.StartNew();
                int count = 0;
                var position = Position.End;
                ReadAllPage page;
                do
                {
                    page = await streamStore.ReadAllBackwards(position,
                        readPageSize,
                        prefetchJsonData: prefetch == YesNo.Yes,
                        cancellationToken: ct);
                    count += page.Messages.Length;
                    Console.Write($"\r> Read {count}");
                    position = page.NextPosition;
                } while (!page.IsEnd);

                stopwatch.Stop();
                var rate = Math.Round((decimal)count / stopwatch.ElapsedMilliseconds * 1000, 0);

                Output.WriteLine("");
                Output.WriteLine($"> {count} messages read in {stopwatch.Elapsed} ({rate} m/s)");
            }
            finally
            {
                dispose();
            }
        }

        private enum YesNo
        {
            Yes,
            No
        }
    }
}