namespace SqlStreamStore
{
    using System.Runtime.CompilerServices;
    using System.Threading.Tasks;

    /// <summary>
    ///     Represents extensions on ValueTasks.
    /// </summary>
    internal static class ValueTaskExtensions
    {
        /// <summary>
        /// ConfigureAwait(false)
        /// </summary>
        public static ConfiguredValueTaskAwaitable<T> NotOnCapturedContext<T>(this ValueTask<T> task)
        {
            return task.ConfigureAwait(false);
        }
    }
}