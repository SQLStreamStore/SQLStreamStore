namespace SqlStreamStore.Streams
{
    /// <summary>
    ///     Represents the result of a read from a stream.
    /// </summary>
    public sealed class StreamMessagesPage
    {
        public readonly StreamMessage[] Messages;
        public readonly int FromStreamVersion;
        public readonly bool IsEnd;
        public readonly int LastStreamVersion;
        public readonly int NextStreamVersion;
        public readonly ReadDirection ReadDirection;
        public readonly PageReadStatus Status;
        public readonly string StreamId;

        public StreamMessagesPage(
            string streamId,
            PageReadStatus status,
            int fromStreamVersion,
            int nextStreamVersion,
            int lastStreamVersion,
            ReadDirection direction,
            bool isEnd,
            params StreamMessage[] messages)
        {
            StreamId = streamId;
            Status = status;
            FromStreamVersion = fromStreamVersion;
            LastStreamVersion = lastStreamVersion;
            NextStreamVersion = nextStreamVersion;
            ReadDirection = direction;
            IsEnd = isEnd;
            Messages = messages;
        }
    }
}