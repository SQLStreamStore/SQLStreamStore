namespace SqlStreamStore
{
    using System;
    using System.Threading;
    using System.Threading.Tasks;
    using SqlStreamStore.Streams;

    /// <summary>
    ///     Represents a readable and writable stream store.
    /// </summary>
    public interface IStreamStore : IReadonlyStreamStore
    {
        /// <summary>
        ///     Appends a collection of messages to a stream. 
        /// </summary>
        /// <remarks>
        ///     Idempotency and concurrency handling is dependent on the choice of expected version and the messages
        ///     to append.
        /// 
        ///     1. When expectedVersion = ExpectedVersion.NoStream and the stream already exists and the collection of
        ///        message IDs are not already in the stream, then <see cref="WrongExpectedVersionException"/> is
        ///        thrown.
        ///     2. When expectedVersion = ExpectedVersion.Any and the collection of messages IDs don't exist in the
        ///        stream, then they are appended
        ///     3. When expectedVersion = ExpectedVersion.Any and the collection of messages IDs exist in the stream,
        ///        then idempotency is applied and nothing happens.
        ///     4. When expectedVersion = ExpectedVersion.Any and of the collection of messages Ids some exist in the 
        ///        stream and some don't then a <see cref="WrongExpectedVersionException"/> will be thrown.
        ///     5. When expectedVersion is specified and the stream current version does not match the 
        ///        collection of message IDs are are checked against the stream in the correct position then the 
        ///        operation is considered idempotent. Otherwise a <see cref="WrongExpectedVersionException"/> will be
        ///        thrown.
        /// </remarks>
        /// <param name="streamId">
        ///     The Stream Id of the stream to append the messages. Must not start with a '$'.
        /// </param>
        /// <param name="expectedVersion">
        ///     The version of the stream that is expected. This is used to control concurrency and idempotency
        ///     concerns. See <see cref="ExpectedVersion"/>.
        /// </param>
        /// <param name="messages">
        ///     The collection of messages to append.
        /// </param>
        /// <param name="cancellationToken">
        ///     The cancellation instruction.
        /// </param>
        /// <returns>A task representing the asynchronous operation.</returns>
        Task<AppendResult> AppendToStream(
            StreamId streamId,
            int expectedVersion,
            NewStreamMessage[] messages,
            CancellationToken cancellationToken = default);

        /// <summary>
        ///     Hard deletes a stream and all of its messages. Deleting a stream will result in a '$stream-deleted'
        ///     message being appended to the '$deleted' stream. See <see cref="Deleted.StreamDeleted"/> for the
        ///     message structure. 
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
        /// <returns>
        ///     A task representing the asynchronous operation.
        /// </returns>
        Task DeleteStream(
            StreamId streamId,
            int expectedVersion = ExpectedVersion.Any,
            CancellationToken cancellationToken = default);

        /// <summary>
        ///     Hard deletes a message from the stream. Deleting a message will result in a '$message-deleted'
        ///     message being appended to the '$deleted' stream. See <see cref="Deleted.MessageDeleted"/> for the
        ///     message structure. 
        /// </summary>
        /// <param name="streamId">
        ///     The stream to delete.
        /// </param>
        /// <param name="messageId">
        ///     The message to delete. If the message doesn't exist then nothing happens.
        /// </param>
        /// <param name="cancellationToken">
        ///     The cancellation instruction.
        /// </param>
        /// <returns>
        ///     A task representing the asynchronous operation.
        /// </returns>
        Task DeleteMessage(
            StreamId streamId,
            Guid messageId,
            CancellationToken cancellationToken = default);

        /// <summary>
        ///     Sets the metadata for a stream.
        /// </summary>
        /// <param name="streamId">The stream Id to whose metadata is to be set.</param>
        /// <param name="expectedStreamMetadataVersion">
        ///     The expected version number of the metadata stream to apply the metadata. Used for concurrency
        ///     handling. Default value is <see cref="ExpectedVersion.Any"/>. If specified and does not match 
        ///     current version then <see cref="WrongExpectedVersionException"/> will be thrown.
        /// </param>
        /// <param name="maxAge">The max age of the messages in the stream in seconds.</param>
        /// <param name="maxCount">The max count of messages in the stream.</param>
        /// <param name="metadataJson">Custom meta data to associate with the stream.</param>
        /// <param name="cancellationToken">
        ///     The cancellation instruction.
        /// </param>
        /// <returns>
        ///     A task representing the asynchronous operation.
        /// </returns>
        Task SetStreamMetadata(
            StreamId streamId,
            int expectedStreamMetadataVersion = ExpectedVersion.Any,
            int? maxAge = null,
            int? maxCount = null,
            string metadataJson = null,
            CancellationToken cancellationToken = default);
    }

    // TODO
    public interface IStreamStore<TReadPage> : IStreamStore, IReadonlyStreamStore<TReadPage> where TReadPage : IReadAllPage
    {
    }
}
