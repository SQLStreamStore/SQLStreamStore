namespace SqlStreamStore
{
    using System;
    using System.Threading.Tasks;

    /// <summary>
    ///     Represents a subscription to a stream.
    /// </summary>
    public interface IStreamSubscription : IDisposable
    {
        /// <summary>
        /// Gets the name of the subscription.
        /// </summary>
        string Name { get; }

        /// <summary>
        /// Gets the stream id the subscription is associated with.
        /// </summary>
        string StreamId { get; }

        /// <summary>
        /// The last stream version processed by the subscription.
        /// </summary>
        int? LastVersion { get; }

        /// <summary>
        /// A task that represents the subscription has been started. Is is usually not necessary to await this
        /// except perhaps in tests and when you subscribe to end of stream.
        /// </summary>
        Task Started { get; }

        /// <summary>
        /// Gets or sets the max count per read the subscription uses when retrieving messages. Larger values
        /// may result in larger payloads and memory usage whereas smaller values will result in more round-trips
        /// to the store. The correct value requires benchmarking of your application.
        /// </summary>
        int MaxCountPerRead { get; set; }
    }
}