namespace LoadTests
{
    using System;
    using System.Collections.Generic;
    using System.Threading;
    using System.Threading.Tasks;
    using Serilog;
    using SqlStreamStore;
    using SqlStreamStore.Streams;

    public class AppendExpectedVersionAnyParallel : PerformanceTest
    {
        public AppendExpectedVersionAnyParallel(Func<Task<IStreamStore>> createStreamStore) 
            : base(createStreamStore)
        {}

        public override async Task Run(CancellationToken ct)
        {
            var streamStore = await CreateStreamStore();

            var tasks = new List<Task>();
            int count = 0;

            for (int i = 0; i < Environment.ProcessorCount; i++)
            {
                var random = new Random();
                var task = Task.Run(async () =>
                {
                    while (!ct.IsCancellationRequested)
                    {
                        try
                        {
                            int streamNumber = random.Next(0, 100);

                            var eventNumber1 = Interlocked.Increment(ref count);
                            var eventNumber2 = Interlocked.Increment(ref count);
                            var newmessages = StreamStoreAcceptanceTests
                                .CreateNewStreamMessages(eventNumber1, eventNumber2);

                            var info = $"{streamNumber} - {newmessages[0].MessageId}," +
                                       $"{newmessages[1].MessageId}";

                            Log.Logger.Information($"Begin {info}");
                            await streamStore.AppendToStream(
                                $"stream-{streamNumber}",
                                ExpectedVersion.Any,
                                newmessages,
                                ct);
                            Log.Logger.Information($"End   {info}");
                            Console.Write($"\r{eventNumber2}");
                        }
                        catch (Exception ex) when (!(ex is TaskCanceledException))
                        {
                            Log.Logger.Error(ex, ex.Message);
                        }
                    }
                }, ct);
                tasks.Add(task);
            }
            await Task.WhenAll(tasks);
        }
    }
}