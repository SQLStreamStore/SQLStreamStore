namespace SqlStreamStore.TestUtils
{
    using System;
    using System.Threading.Tasks;

    public static class TaskExtensions
    {
        private const int DefaultTimeout = 10000;

        public static async Task<T> WithTimeout<T>(this Task<T> task, int timeout = DefaultTimeout) =>
            await Task.WhenAny(task, Task.Delay(timeout)) == task
                ? task.Result
                : throw new TimeoutException("Timed out waiting for task");

        public static async Task WithTimeout(this Task task, int timeout = DefaultTimeout)
        {
            if(await Task.WhenAny(task, Task.Delay(timeout)) != task)
                throw new TimeoutException("Timed out waiting for task");
        }

        public static Task WithTimeout(this ValueTask task, int timeout = DefaultTimeout)
            => task.AsTask().WithTimeout(timeout);

        public static Task<T> WithTimeout<T>(this ValueTask<T> task, int timeout = DefaultTimeout)
            => task.AsTask().WithTimeout(timeout);
    }
}