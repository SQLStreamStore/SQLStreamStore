using System;
using System.Threading.Tasks;

namespace SqlStreamStore
{
    public static class TaskExtensions
    {
        public static async Task<T> WithTimeout<T>(this Task<T> task, int timeout = 3000)
        {
            if(await Task.WhenAny(task, Task.Delay(timeout)) == task)
            {
                return await task;
            }

            throw new TimeoutException("Timed out waiting for task");
        }

        public static async Task WithTimeout(this Task task, int timeout = 3000)
        {
            if(await Task.WhenAny(task, Task.Delay(timeout)) == task)
            {
                await task;
                return;
            }

            throw new TimeoutException("Timed out waiting for task");
        }
    }
}