namespace SqlStreamStore.V1.Infrastructure
{
    using System.Collections.Generic;
    using System.Runtime.CompilerServices;
    using System.Threading.Tasks;

    /// <summary>
    ///     Represents extensions on Tasks.
    /// </summary>
    public static class TaskExtensions
    {
        /// <summary>
        /// ConfigureAwait(false)
        /// </summary>
        public static ConfiguredTaskAwaitable NotOnCapturedContext(this Task task)
        {
            return task.ConfigureAwait(false);
        }

        /// <summary>
        /// ConfigureAwait(false)
        /// </summary>
        public static ConfiguredTaskAwaitable<T> NotOnCapturedContext<T>(this Task<T> task)
        {
            return task.ConfigureAwait(false);
        }

        /// <summary>
        /// ConfigureAwait(false)
        /// </summary>
        public static ConfiguredCancelableAsyncEnumerable<T> NotOnCapturedContext<T>(this IAsyncEnumerable<T> enumerable)
        {
            return enumerable.ConfigureAwait(false);
        }
    }
}