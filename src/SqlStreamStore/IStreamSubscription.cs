namespace SqlStreamStore
{
    using System;
    using System.Threading.Tasks;

    public interface IStreamSubscription : IDisposable
    {
        /// <summary>
        /// Gets the subscription name.
        /// </summary>
        string Name { get; }

        /// <summary>
        /// Gets the stream Id of the subscripton.
        /// </summary>
        string StreamId { get; }

        /// <summary>
        /// Gets the last version handled by the subscription.
        /// </summary>
        int LastVersion { get; }

        /// <summary>
        /// Gets a task 
        /// </summary>
        Task Started { get; }

        /// <summary>
        /// Gets or sets the size of the page used when reading from 
        /// </summary>
        int PageSize { get; set; }
    }
}