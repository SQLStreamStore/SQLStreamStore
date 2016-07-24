namespace SqlStreamStore
{
    using System;
    using System.Threading;
    using System.Threading.Tasks;
    using SqlStreamStore.Streams;

    public interface IStreamStore : IReadonlyStreamStore
    {
        /// <summary>
        ///     Appends a collection of Messages to a stream. 
        /// </summary>
        /// <param name="streamId">
        ///     The Stream Id to append Messages to. Must not start with a '$'.
        /// </param>
        /// <param name="expectedVersion">
        ///     The version of the stream that is expected. This is used to control concurrency concerns. See
        ///     <see cref="ExpectedVersion"/>.
        /// </param>
        /// <param name="messages">
        ///     The collection of Messages to append.
        /// </param>
        /// <param name="cancellationToken">
        ///     The cancellation instruction.
        /// </param>
        /// <returns>A task representing the asynchronous operation.</returns>
        /// <exception cref="WrongExpectedVersionException">Thrown </exception>
        Task AppendToStream(
            string streamId,
            int expectedVersion,
            NewStreamMessage[] messages,
            CancellationToken cancellationToken = default(CancellationToken));

        /// <summary>
        ///     Hard deletes a stream and all of its Messages. Deleting a stream will result in a '$stream-deleted'
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
        ///     message being appended to the '$deleted' stream.
        /// </summary>
        /// <param name="streamId">
        ///     The stream Id to delete.
        /// </param>
        /// <param name="messageId">
        ///     The Id of the message to delete. If the message  doesn't exist, nothing occurs.
        /// </param>
        /// <param name="cancellationToken">
        ///     The cancellation instruction.
        /// </param>
        /// <returns>
        ///     A task representing the asynchronous operation.
        /// </returns>
        Task DeleteMessage(
            string streamId,
            Guid messageId,
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