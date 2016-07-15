namespace Cedar.EventStore.Infrastructure
{
    using System;
    using System.Collections.Concurrent;
    using System.Threading;
    using System.Threading.Tasks;

    public class TaskQueue : IDisposable
    {
        private readonly ConcurrentQueue<Func<Task>> _taskQueue = new ConcurrentQueue<Func<Task>>();
        private readonly ConcurrentQueue<Func<Task>> _highPriorityTaskQueue = new ConcurrentQueue<Func<Task>>();
        private readonly CancellationTokenSource _isDisposed = new CancellationTokenSource();
        private readonly InterlockedBoolean _isProcessing = new InterlockedBoolean();

        public Task Enqueue(Action action)
        {
            var task = Enqueue(_ =>
            {
                action();
                return TaskHelpers.CompletedTask;
            });
            return task;
        }

        public Task Enqueue(Func<CancellationToken, Task> function)
        {
            var task = Enqueue(async ct =>
            {
                await function(ct);
                return true;
            });
            return task;
        }

        public Task<TResult> Enqueue<TResult>(Func<CancellationToken, Task<TResult>> function)
        {
            return EnqueueInternal(_taskQueue, function);
        }

        public Task EnqueueHighPriority(Action action)
        {
            var task = EnqueueHighPriority(_ =>
            {
                action();
                return TaskHelpers.CompletedTask;
            });
            return task;
        }

        public Task EnqueueHighPriority(Func<CancellationToken, Task> function)
        {
            var task = EnqueueHighPriority(async ct =>
            {
                await function(ct);
                return true;
            });
            return task;
        }

        public Task<TResult> EnqueueHighPriority<TResult>(Func<CancellationToken, Task<TResult>> function)
        {
            return EnqueueInternal(_highPriorityTaskQueue, function);
        }

        private Task<TResult> EnqueueInternal<TResult>(
            ConcurrentQueue<Func<Task>> taskQueue,
            Func<CancellationToken, Task<TResult>> function)
        {
            var tcs = new TaskCompletionSource<TResult>();
            if (_isDisposed.IsCancellationRequested)
            {
                tcs.SetCanceled();
                return tcs.Task;
            }
            taskQueue.Enqueue(async () =>
            {
                if (_isDisposed.IsCancellationRequested)
                {
                    tcs.SetCanceled();
                    return;
                }
                try
                {
                    var result = await function(_isDisposed.Token);
                    tcs.SetResult(result);
                }
                catch (TaskCanceledException)
                {
                    tcs.SetCanceled();
                }
                catch (Exception ex)
                {
                    tcs.SetException(ex);
                }

            });
            if (_isProcessing.CompareExchange(true, false) == false)
            {
                Task.Run(ProcessTaskQueue);
            }
            return tcs.Task;
        }

        private async Task ProcessTaskQueue()
        {
            do
            {
                Func<Task> function;
                if(_highPriorityTaskQueue.TryDequeue(out function))
                {
                    await function();
                }
                else if(_taskQueue.TryDequeue(out function))
                {
                    await function();
                }
                _isProcessing.Set(false);
            } while( (_highPriorityTaskQueue.Count > 0 || _taskQueue.Count > 0) 
                && _isProcessing.CompareExchange(true, false) == false);
        }

        public void Dispose()
        {
            _isDisposed.Cancel();
        }
    }
}
