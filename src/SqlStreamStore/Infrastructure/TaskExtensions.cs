namespace SqlStreamStore.Infrastructure
{
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
        public static ConfiguredValueTaskAwaitable NotOnCapturedContext(this ValueTask task)
        {
            return task.ConfigureAwait(false);
        }

        /// <summary>
        /// ConfigureAwait(false)
        /// </summary>
        public static ConfiguredValueTaskAwaitable<T> NotOnCapturedContext<T>(this ValueTask<T> task)
        {
            return task.ConfigureAwait(false);
        }
    }
}