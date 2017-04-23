namespace SqlStreamStore
{
    using System;
    using System.Threading.Tasks;

    /// <summary>
    ///     Represents a subscript to all streams.
    /// </summary>
    /// <seealso cref="System.IDisposable" />
    public interface IAllStreamSubscription : IDisposable
    {
        /// <summary>
        /// Gets the name of the subscription. Useful for debugging and diagnostics.
        /// </summary>
        string Name { get; }

        /// <summary>
        /// The last position processed by the subscription. Will be -1 if nothing has yet been processed.
        /// </summary>
        long? LastPosition { get; }

        /// <summary>
        /// A task that represents the subscription has been started. Is is usually not necessary to await this
        /// except perhaps in tests when you subscribe to end of all stream.
        /// </summary>
        Task Started { get; }

        /// <summary>
        /// Gets or sets the max count per read the subscription uses when retrieving messages. Larger values
        /// may result in larger payloads and memory usage whereas smaller values will result in more round-trips
        /// to the store. The correct value requires benchmarking of your application.
        /// </summary>
        int PageSizePerRead { get; set; }
    }
}