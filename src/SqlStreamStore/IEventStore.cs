namespace SqlStreamStore
{
    using System;
    using System.Threading;
    using System.Threading.Tasks;
    using SqlStreamStore.Streams;

    public interface IEventStore : IReadOnlyEventStore
    {
        /// <summary>
        ///     Appends a collection of events to a stream. 
        /// </summary>
        /// <param name="streamId">
        ///     The Stream Id to append events to. Must not start with a '$'.
        /// </param>
        /// <param name="expectedVersion">
        ///     The version of the stream that is expected. This is used to control concurrency concerns. See
        ///     <see cref="ExpectedVersion"/>.
        /// </param>
        /// <param name="events">
        ///     The collection of events to append.
        /// </param>
        /// <param name="cancellationToken">
        ///     The cancellation instruction.
        /// </param>
        /// <returns>A task representing the asynchronous operation.</returns>
        /// <exception cref="WrongExpectedVersionException">Thrown </exception>
        Task AppendToStream(
            string streamId,
            int expectedVersion,
            NewStreamEvent[] events,
            CancellationToken cancellationToken = default(CancellationToken));

        /// <summary>
        ///     Hard deletes a stream and all of its events. Deleting a stream will result in a '$stream-deleted'
        ///     event being appended to the '$deleted' stream.
        /// </summary>
        /// <param name="streamId">
        ///     The stream Id to delete.
        /// </param>
        /// <param name="expectedVersion">
        ///     The stream expected version. See <see cref="ExpectedVersion"/> for const values.
        /// </param>
        /// <param name="cancellationToken">
        ///     The cancellation instruction.
        /// </param>
        /// <returns>A task representing the asynchronous operation.</returns>
        Task DeleteStream(
            string streamId,
            int expectedVersion = ExpectedVersion.Any,
            CancellationToken cancellationToken = default(CancellationToken));

        /// <summary>
        ///     Hard deletes an event from the stream. Deleting an event message will result in an '$event-deleted'
        ///     event being appended to the '$deleted' stream.
        /// </summary>
        /// <param name="streamId">
        ///     The stream Id to delete.
        /// </param>
        /// <param name="eventId">
        ///     The Id of the event to delete. If the event doesn't exist, nothing occurs.
        /// </param>
        /// <param name="cancellationToken">
        ///     The cancellation instruction.
        /// </param>
        /// <returns>
        ///     A task representing the asynchronous operation.
        /// </returns>
        Task DeleteEvent(
            string streamId,
            Guid eventId,
            CancellationToken cancellationToken = default(CancellationToken));

        Task SetStreamMetadata(
            string streamId,
            int expectedStreamMetadataVersion = ExpectedVersion.Any,
            int? maxAge = null,
            int? maxCount = null,
            string metadataJson = null,
            CancellationToken cancellationToken = default(CancellationToken));
    }
}