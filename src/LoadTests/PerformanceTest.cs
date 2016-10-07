namespace LoadTests
{
    using System;
    using System.Threading;
    using System.Threading.Tasks;
    using SqlStreamStore;

    public abstract class PerformanceTest
    {
        protected readonly Func<Task<IStreamStore>> CreateStreamStore;

        protected PerformanceTest(Func<Task<IStreamStore>> createStreamStore)
        {
            CreateStreamStore = createStreamStore;
        }

        public abstract Task Run(CancellationToken cancellationToken);
    }
}