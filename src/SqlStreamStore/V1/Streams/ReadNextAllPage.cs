namespace SqlStreamStore.V1.Streams
{
    using System.Threading;
    using System.Threading.Tasks;

    /// <summary>
    ///     Represents an operation to read the next all page.
    /// </summary>
    /// <param name="nextPosition">The position to read from.</param>
    /// <param name="cancellationToken">A token to cancel the operation.</param>
    /// <returns>A task that represents the result of the operation.z</returns>
    public delegate Task<ReadAllPage> ReadNextAllPage(long nextPosition, CancellationToken cancellationToken);
}