using System;
using System.Threading.Tasks;

namespace SqlStreamStore
{
    public static class TaskExtensions
    {
        public static async Task<T> WithTimeout<T>(this Task<T> task, int timeout = 3000) =>
            await Task.WhenAny(task, Task.Delay(timeout)) == task
                ? task.Result
                : throw new TimeoutException("Timed out waiting for task");

        public static async Task WithTimeout(this Task task, int timeout = 3000)
        {
            if(await Task.WhenAny(task, Task.Delay(timeout)) != task)
                throw new TimeoutException("Timed out waiting for task");
        }
    }
}