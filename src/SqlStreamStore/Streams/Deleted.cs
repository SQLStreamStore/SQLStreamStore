namespace SqlStreamStore.Streams
{
    using System;
    using StreamStoreStore.Json;

    public static class Deleted
    {
        public const string DeletedStreamId = "$deleted";

        public const string StreamDeletedEventType = "$stream-deleted";

        public const string EventDeletedEventType = "$event-deleted";

        public static NewStreamMessage CreateStreamDeletedEvent(string streamId)
        {
            var streamDeleted = new StreamDeleted { StreamId = streamId };
            var eventJson = SimpleJson.SerializeObject(streamDeleted);

            return new NewStreamMessage(Guid.NewGuid(), StreamDeletedEventType, eventJson);
        }

        public static NewStreamMessage CreateEventDeletedEvent(string streamId, Guid eventId)
        {
            var eventDeleted = new EventDeleted { StreamId = streamId, EventId = eventId };
            var eventJson = SimpleJson.SerializeObject(eventDeleted);

            return new NewStreamMessage(Guid.NewGuid(), EventDeletedEventType, eventJson);
        }

        public class StreamDeleted
        {
            public string StreamId;
        }

        public class EventDeleted
        {
            public string StreamId;
            public Guid EventId;
        }
    }
}