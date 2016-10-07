namespace LoadTests
{
    using System;
    using System.Threading;
    using System.Threading.Tasks;
    using SqlStreamStore;
    using SqlStreamStore.Streams;

    public class AppendExpectedVersionAnySingleThread : PerformanceTest
    {
        public AppendExpectedVersionAnySingleThread(Func<Task<IStreamStore>> createStreamStore) 
            : base(createStreamStore)
        {}

        public override async Task Run(CancellationToken ct)
        {
            var streamStore = await CreateStreamStore();
            var random = new Random();
            int eventNumber = 1;
            int loopCount = 0;
            while (!ct.IsCancellationRequested)
            {
                try
                {
                    int streamNumber = random.Next(0, 100);

                    var eventNumber1 = eventNumber++;
                    var eventNumber2 = eventNumber++;
                    var newmessages = StreamStoreAcceptanceTests
                        .CreateNewStreamMessages(eventNumber1, eventNumber2);

                    await streamStore.AppendToStream(
                        $"stream-{streamNumber}",
                        ExpectedVersion.Any,
                        newmessages,
                        ct);
                    if(loopCount%100 == 0)
                    {
                        Console.Write($"\r{loopCount}");
                    }
                    loopCount++;
                }
                catch (Exception ex) when (!(ex is TaskCanceledException))
                {
                    Console.WriteLine(ex);
                }
            }
        }
    }
}