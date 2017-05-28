namespace SqlStreamStore.Infrastructure
{
    using System;
    using System.Collections.Concurrent;
    using System.Threading;
    using System.Threading.Tasks;

    /// <summary>
    ///     Represents a queue of tasks where a task is processed one at a time. When disposed
    ///     the outstanding tasks are cancelled.
    /// </summary>
    public class TaskQueue : IDisposable
    {
        private readonly ConcurrentQueue<Func<Task>> _taskQueue = new ConcurrentQueue<Func<Task>>();
        private readonly CancellationTokenSource _isDisposed = new CancellationTokenSource();
        private readonly InterlockedBoolean _isProcessing = new InterlockedBoolean();


        /// <summary>
        ///     Enqueues a task for processing.
        /// </summary>
        /// <param name="action">The operations to invoke.</param>
        /// <returns>A task representing the operation. Awaiting is optional.</returns>
        public Task Enqueue(Action action)
        {
            var task = Enqueue(_ =>
            {
                action();
                return Task.CompletedTask;
            });
            return task;
        }

        /// <summary>
        ///     Enqueues a task for processing.
        /// </summary>
        /// <param name="function">The operations to invoke.</param>
        /// <returns>A task representing the operation. Awaiting is optional.</returns>
        public Task<T> Enqueue<T>(Func<T> function)
        {
            var task = Enqueue(_ =>
            {
                var result = function();
                return Task.FromResult(result);
            });
            return task;
        }

        /// <summary>
        ///     Enqueues a task for processing.
        /// </summary>
        /// <param name="function">The operation to invoke that is co-operatively cancelable.</param>
        /// <returns>A task representing the operation. Awaiting is optional.</returns>
        public Task Enqueue(Func<CancellationToken, Task> function)
        {
            var task = Enqueue(async ct =>
            {
                await function(ct);
                return true;
            });
            return task;
        }

        /// <summary>
        ///     Enqueues a task for processing.
        /// </summary>
        /// <param name="function">The operation to invoke that is co-operatively cancelable.</param>
        /// <returns>A task representing the operation. Awaiting is optional.</returns>
        public Task<TResult> Enqueue<TResult>(Func<CancellationToken, Task<TResult>> function)
        {
            return EnqueueInternal(_taskQueue, function);
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
                if (_taskQueue.TryDequeue(out Func<Task> function))
                {
                    await function();
                }
                _isProcessing.Set(false);
            } while( _taskQueue.Count > 0 && _isProcessing.CompareExchange(true, false) == false);
        }

        public void Dispose()
        {
            _isDisposed.Cancel();
        }
    }
}
