namespace Cedar.EventStore.Infrastructure
{
    // From http://blogs.msdn.com/b/pfxteam/archive/2011/01/15/asynclazy-lt-t-gt.aspx

    using System;
    using System.Threading.Tasks;

    public class AsyncLazy<T> : Lazy<Task<T>>
    {
        public AsyncLazy(Func<Task<T>> taskFactory, bool isThreadSafe) :
            base(() => Task.Factory.StartNew(taskFactory).Unwrap(), isThreadSafe)
        {}
    }
}