namespace Cedar.EventStore.Streams
{
    using System;

    public static class Deleted
    {
        public static string StreamId = "$deleted";

        public static string StreamDeletedEventType = "$stream-deleted";

        public static string EventDeletedEventType = "$event-deleted";

        public static NewStreamEvent CreateStreamDeletedEvent(string streamId)
        {
            var eventJson = $"{{ \"streamId\": \"{streamId}\" }}";
            return new NewStreamEvent(Guid.NewGuid(), StreamDeletedEventType, eventJson);
        }
    }
}