namespace Cedar.EventStore.Infrastructure
{
    using System;
    using System.Diagnostics;
    using System.Threading.Tasks;
    using Cedar.EventStore.Logging;

    internal static class OperationExtensions
    {
        internal static async Task<T> Measure<T>(this Func<Task<T>> operation, ILog logger, Func<T, long, string> generateMessage)
        {
            if(!logger.IsDebugEnabled())
            {
                return await operation();
            }
            var stopWatch = Stopwatch.StartNew();
            T result = await operation();
            logger.Debug(generateMessage(result, stopWatch.ElapsedMilliseconds));
            return result;
        }
    }
}