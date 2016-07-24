namespace SqlStreamStore.Streams
{
    using System;
    using StreamStoreStore.Json;

    public static class Deleted
    {
        public const string DeletedStreamId = "$deleted";

        public const string StreamDeletedMessageType = "$stream-deleted";

        public const string MessageDeletedMessageType = "$message-deleted";

        public static NewStreamMessage CreateStreamDeletedEvent(string streamId)
        {
            var streamDeleted = new StreamDeleted { StreamId = streamId };
            var eventJson = SimpleJson.SerializeObject(streamDeleted);

            return new NewStreamMessage(Guid.NewGuid(), StreamDeletedMessageType, eventJson);
        }

        public static NewStreamMessage CreateEventDeletedEvent(string streamId, Guid messageId)
        {
            var eventDeleted = new MessageDeleted { StreamId = streamId, MessageId = messageId };
            var eventJson = SimpleJson.SerializeObject(eventDeleted);

            return new NewStreamMessage(Guid.NewGuid(), MessageDeletedMessageType, eventJson);
        }

        public class StreamDeleted
        {
            public string StreamId;
        }

        public class MessageDeleted
        {
            public string StreamId;
            public Guid MessageId;
        }
    }
}