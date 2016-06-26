namespace Cedar.EventStore
{
    using System.Threading;
    using System.Threading.Tasks;
    using Cedar.EventStore.Streams;

    public interface IEventStore : IReadOnlyEventStore
    {
        Task AppendToStream(
            string streamId,
            int expectedVersion,
            NewStreamEvent[] events,
            CancellationToken cancellationToken = default(CancellationToken));

        /// <summary>
        ///     Hard deletes a stream and all of its events. Deleting a stream will result in a '$stream-deleted'
        ///     event being appended to the '$deleted' stream.
        /// </summary>
        /// <param name="streamId">The stream Id to delete</param>
        /// <param name="expectedVersion">The stream expected version. If it does not match, a
        ///     <see cref="WrongExpectedVersionException"/> will be thrown.</param>
        /// <param name="cancellationToken">A cancellation token to cancel the operations.</param>
        /// <returns></returns>
        Task DeleteStream(
            string streamId,
            int expectedVersion = ExpectedVersion.Any,
            CancellationToken cancellationToken = default(CancellationToken));      
    }
}