namespace SqlStreamStore.V1.Streams
{
    using System.Threading;
    using System.Threading.Tasks;

    /// <summary>
    ///     Represents an operaion to read the next stream page.
    /// </summary>
    /// <param name="nextVersion">The next version to read from.</param>
    /// <param name="cancellationToken">A token to cancel the operation.</param>
    /// <returns>A task representing the result of the reading the next stream page.</returns>
    public delegate Task<ReadStreamPage> ReadNextStreamPage(int nextVersion, CancellationToken cancellationToken);
}