namespace SqlStreamStore.V1.Streams
{
    using System;

    /// <summary>
    ///     Represents information regarding deleted messages and streams.
    /// </summary>
    public static class Deleted
    {
        /// <summary>
        ///     The Id of the stream that delete messages are written to.
        /// </summary>
        public const string DeletedStreamId = "$deleted";

        /// <summary>
        ///     The message type of a stream deleted message.
        /// </summary>
        public const string StreamDeletedMessageType = "$stream-deleted";

        /// <summary>
        ///     The message type of a message deleted message.
        /// </summary>
        public const string MessageDeletedMessageType = "$message-deleted";

        /// <summary>
        ///     Creates a <see cref="NewStreamMessage"/> that contains a stream deleted message.
        /// </summary>
        /// <param name="streamId">The stream id of the deleted stream.</param>
        /// <returns>A <see cref="NewStreamMessage"/>.</returns>
        public static NewStreamMessage CreateStreamDeletedMessage(string streamId)
        {
            var streamDeleted = new StreamDeleted { StreamId = streamId };
            var json = SimpleJson.SerializeObject(streamDeleted);

            return new NewStreamMessage(Guid.NewGuid(), StreamDeletedMessageType, json);
        }

        /// <summary>
        ///     Creates a <see cref="NewStreamMessage"/> that contains a message deleted message.
        /// </summary>
        /// <param name="streamId">The stream id of the deleted stream.</param>
        /// <param name="messageId">The message id of the deleted message.</param>
        /// <returns>A <see cref="NewStreamMessage"/>A <see cref="NewStreamMessage"/>.</returns>
        public static NewStreamMessage CreateMessageDeletedMessage(string streamId, Guid messageId)
        {
            var messageDeleted = new MessageDeleted { StreamId = streamId, MessageId = messageId };
            var json = SimpleJson.SerializeObject(messageDeleted);

            return new NewStreamMessage(Guid.NewGuid(), MessageDeletedMessageType, json);
        }

        /// <summary>
        ///     The message appended to $deleted when a stream is deleted.
        /// </summary>
        public class StreamDeleted
        {
            /// <summary>
            ///     The stream id the deleted of the deleted stream. 
            /// </summary>
            public string StreamId;
        }

        /// <summary>
        ///     The message appended to $deleted with an individual message is deleted.
        /// </summary>
        public class MessageDeleted
        {
            /// <summary>
            ///     The stream id the deleted message belonged to. 
            /// </summary>
            public string StreamId;

            /// <summary>
            ///     The message id of the deleted message.
            /// </summary>
            public Guid MessageId;
        }
    }
}