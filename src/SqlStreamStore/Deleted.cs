namespace SqlStreamStore
{
    using System;
    using StreamStoreStore.Json;

    public static class Deleted
    {
        public const string DeletedStreamId = "$deleted";

        public const string StreamDeletedMessageType = "$stream-deleted";

        public const string MessageDeletedMessageType = "$message-deleted";

        public static NewStreamMessage CreateStreamDeletedMessage(string streamId)
        {
            var streamDeleted = new StreamDeleted { StreamId = streamId };
            var json = SimpleJson.SerializeObject(streamDeleted);

            return new NewStreamMessage(Guid.NewGuid(), StreamDeletedMessageType, json);
        }

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
            public string StreamId;
        }

        /// <summary>
        ///     The message appended to $deleted with an individual message is deleted.
        /// </summary>
        public class MessageDeleted
        {
            public string StreamId;
            public Guid MessageId;
        }
    }
}