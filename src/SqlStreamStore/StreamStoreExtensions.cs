namespace SqlStreamStore
{
    using System.Collections.Generic;
    using System.Linq;
    using System.Threading;
    using System.Threading.Tasks;
    using SqlStreamStore.Streams;

    public static class StreamStoreExtensions
    {
        /// <summary>
        ///     Reads messages from all streams forwards.
        /// </summary>
        /// <param name="fromPositionInclusive">
        ///     The position to start reading from. Use <see cref="Position.Start"/> to start from the beginning.
        ///     Note: messages that have expired will be filtered out.
        /// </param>
        /// <param name="maxCount">
        ///     The maximum number of messages to read (int.MaxValue is a bad idea).
        /// </param>
        /// <param name="cancellationToken">
        ///     The cancellation instruction.
        /// </param>
        /// <returns>
        ///     An <see cref="ReadAllPage"/> presenting the result of the read. If all messages read have expired
        ///     then the message collection MAY be empty.
        /// </returns>
        public static Task<ReadAllPage> ReadAllForwards(
            this IReadonlyStreamStore readonlyStreamStore,
            long fromPositionInclusive,
            int maxCount,
            CancellationToken cancellationToken = default(CancellationToken))
        {
            return readonlyStreamStore.ReadAllForwards(fromPositionInclusive,
                maxCount,
                cancellationToken: cancellationToken);
        }

        /// <summary>
        ///     Appends a collection of messages to a stream. 
        /// </summary>
        /// <remarks>
        ///     Idempotency and concurrency handling is dependent on the choice of expected version and the messages
        ///     to append.
        /// 
        ///     1. When expectedVersion = ExpectedVersion.NoStream and the stream already exists and the collection of
        ///        message IDs are not already in the stream, then <see cref="WrongExpectedVersionException"/> is
        ///        throw.
        ///     2. When expectedVersion = ExpectedVersion.Any and the collection of messages IDs don't exist in the
        ///        stream, then they are appended
        ///     3. When expectedVersion = ExpectedVersion.Any and the collection of messages IDs exist in the stream,
        ///        then idempotency is applied and nothing happens.
        ///     4. When expectedVersion = ExpectedVersion.Any and of the collection of messages Ids some exist in the 
        ///        stream and some don't then a <see cref="WrongExpectedVersionException"/> will be throwm.
        ///     5. When expectedVersion is specified and the stream current version does not match the 
        ///        collection of message IDs are are checked against the stream in the correct position then the 
        ///        operation is considered idempotent. Otherwise a <see cref="WrongExpectedVersionException"/> will be
        ///        throwm.
        /// </remarks>
        /// <param name="store">
        ///     The stream store instance.
        /// </param>
        /// <param name="streamId">
        ///     The Stream Id of the stream to append the messages. Must not start with a '$'.
        /// </param>
        /// <param name="expectedVersion">
        ///     The version of the stream that is expected. This is used to control concurrency and idempotency
        ///     concerns. See <see cref="ExpectedVersion"/>.
        /// </param>
        /// <param name="message">
        ///     The messages to append.
        /// </param>
        /// <param name="cancellationToken">
        ///     The cancellation instruction.
        /// </param>
        /// <returns>A task representing the asynchronous operation.</returns>
        public static Task AppendToStream(
            this IStreamStore store,
            string streamId,
            int expectedVersion,
            NewStreamMessage message,
            CancellationToken cancellationToken = default(CancellationToken))
        {
            return store.AppendToStream(streamId, expectedVersion, new[] { message }, cancellationToken);
        }

        /// <summary>
        ///     Appends a collection of messages to a stream. 
        /// </summary>
        /// <remarks>
        ///     Idempotency and concurrency handling is dependent on the choice of expected version and the messages
        ///     to append.
        /// 
        ///     1. When expectedVersion = ExpectedVersion.NoStream and the stream already exists and the collection of
        ///        message IDs are not already in the stream, then <see cref="WrongExpectedVersionException"/> is
        ///        throw.
        ///     2. When expectedVersion = ExpectedVersion.Any and the collection of messages IDs don't exist in the
        ///        stream, then they are appended
        ///     3. When expectedVersion = ExpectedVersion.Any and the collection of messages IDs exist in the stream,
        ///        then idempotency is applied and nothing happens.
        ///     4. When expectedVersion = ExpectedVersion.Any and of the collection of messages Ids some exist in the 
        ///        stream and some don't then a <see cref="WrongExpectedVersionException"/> will be throwm.
        ///     5. When expectedVersion is specified and the stream current version does not match the 
        ///        collection of message IDs are are checked against the stream in the correct position then the 
        ///        operation is considered idempotent. Otherwise a <see cref="WrongExpectedVersionException"/> will be
        ///        throwm.
        /// </remarks>
        /// <param name="store">
        ///     The stream store instance.
        /// </param>
        /// <param name="streamId">
        ///     The Stream Id of the stream to append the messages. Must not start with a '$'.
        /// </param>
        /// <param name="expectedVersion">
        ///     The version of the stream that is expected. This is used to control concurrency and idempotency
        ///     concerns. See <see cref="ExpectedVersion"/>.
        /// </param>
        /// <param name="messages">
        ///     The messages to append.
        /// </param>
        /// <param name="cancellationToken">
        ///     The cancellation instruction.
        /// </param>
        /// <returns>A task representing the asynchronous operation.</returns>
        public static Task AppendToStream(
            this IStreamStore store,
            string streamId,
            int expectedVersion,
            IEnumerable<NewStreamMessage> messages,
            CancellationToken cancellationToken = default(CancellationToken))
        {
            return store.AppendToStream(streamId, expectedVersion, messages.ToArray(), cancellationToken);
        }
    }
}