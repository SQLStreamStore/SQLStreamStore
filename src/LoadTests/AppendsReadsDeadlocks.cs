namespace LoadTests
{
    using System;
    using System.Collections.Concurrent;
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.Linq;
    using System.Threading;
    using System.Threading.Tasks;
    using SqlStreamStore;
    using SqlStreamStore.Streams;

    public class AppendsReadsDeadlocks : LoadTest
    {
        public override async Task Run(CancellationToken ct)
        {
            int chunksCount = 1000;
            int chunkSize = 100;
            var (store, dispose) = await GetStore(ct);

            Task saveTask = GenerateMessages(store, chunksCount, chunkSize);
            Task readTask = GetManyPagesAsync(store, chunksCount, chunkSize);

            await Task.WhenAll(saveTask, readTask);

            dispose();
        }

        private static async Task GenerateMessages(IStreamStore store, int chunksCount, int chunkSize)
        {
            var dictStreamMessage = new Dictionary<string, NewStreamMessage>();
            var random = new Random();
            var messageJsonDataSize = 50 * 1024;
            for (int n = 0; n < chunksCount; n++)
            {
                var stopwatch = Stopwatch.StartNew();
                for (int i = 0; i < chunkSize; i++) //generate chunk of messages
                {
                    string jsonData = $"message-{n * chunksCount + i}" + new string('m', random.Next(messageJsonDataSize));
                    var message = new NewStreamMessage(Guid.NewGuid(), jsonData, "{}", $"{i}");
                    var streamId = $"streamId{random.Next(n * chunksCount + i)}";
                    dictStreamMessage[streamId] = message;
                }
                //await -in-parallel
                await dictStreamMessage.ForEachAsync(chunkSize,
                    async kvp => { await store.AppendToStream(kvp.Key, ExpectedVersion.Any, kvp.Value); },
                    t =>
                    {
                        //will be called only if t.IsFaulted  
                        var exception = t.Exception;
                        var errorMessage = $"Task {t.Id} failed " + exception?.GetBaseException().Message;
                        throw new Exception(errorMessage, exception);
                    });
                Console.WriteLine($"Chunk number {n} saved. Elapsed {stopwatch.Elapsed} ");
            }
        }

        public async Task<long> GetManyPagesAsync(IStreamStore<IReadAllPage> store, int chunksCount, int batchSize)
        {
            long start = 0;
            //var events = new List<StreamMessage>();
            for (int i = 0; i < chunksCount; i++)
            {
                var stopwatch = Stopwatch.StartNew();
                var timesSlept = 0;
                bool moreThanPage = false;
                while (!moreThanPage)
                {
                    var page = await store.ReadAllForwards(start, batchSize);
                    if (page.IsEnd)
                    {

                        Thread.Sleep(10);
                        timesSlept++;
                    }
                    else
                    {
                        start = page.NextPosition;
                        moreThanPage = true;
                        Console.WriteLine($"Page start from {start} read. Slept {timesSlept} times by 10ms.Elapsed {stopwatch.Elapsed}");
                    }
                }
            }

            return start;
        }
    }

    public static class CollectionAsyncExtensions
    {
        //Consider to use AsyncEnumerable instead

        /// <summary>
        /// https://stackoverflow.com/questions/11564506/nesting-await-in-parallel-foreach/25877042#25877042
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="source">Should have more records than degreeOfParallelism</param>
        /// <param name="degreeOfParallelism"></param>
        /// <param name="body"></param>
        /// <param name="handleException"></param>
        /// <returns></returns>
        public static async Task ForEachAsync<T>(
            this ICollection<T> source, int degreeOfParallelism, Func<T, Task> body, Action<Task> handleException = null)
        {
            if (source.Count > 0)
            {
                await Task.WhenAll(
                    from partition in Partitioner.Create(source).GetPartitions(degreeOfParallelism)
                    select Task.Run(async delegate
                    {
                        using (partition)
                            while (partition.MoveNext())
                                await body(partition.Current).ContinueWith(t =>
                                {
                                    //observe exceptions
                                    if (t.IsFaulted)
                                    {
                                        handleException?.Invoke(t);
                                    }
                                });
                    }));
            }
        }
    }
}