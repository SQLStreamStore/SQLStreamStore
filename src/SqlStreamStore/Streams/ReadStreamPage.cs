namespace SqlStreamStore.Streams
{
    using System;
    using System.Threading;
    using System.Threading.Tasks;

    /// <summary>
    ///     Represents the result of a read from a stream.
    /// </summary>
    public sealed class ReadStreamPage
    {
        public readonly StreamMessage[] Messages;
        private readonly ReadNextStreamPage _readNext;
        public readonly int FromStreamVersion;
        public readonly bool IsEnd;
        public readonly int LastStreamVersion;
        public readonly int NextStreamVersion;
        public readonly ReadDirection ReadDirection;
        public readonly PageReadStatus Status;
        public readonly string StreamId;

        public ReadStreamPage(
            string streamId,
            PageReadStatus status,
            int fromStreamVersion,
            int nextStreamVersion,
            int lastStreamVersion,
            ReadDirection direction,
            bool isEnd,
            StreamMessage[] messages,
            ReadNextStreamPage readNext = null)
        {
            StreamId = streamId;
            Status = status;
            FromStreamVersion = fromStreamVersion;
            LastStreamVersion = lastStreamVersion;
            NextStreamVersion = nextStreamVersion;
            ReadDirection = direction;
            IsEnd = isEnd;
            Messages = messages;
            _readNext = readNext ?? ((_, __) =>
                        {
                            throw new NotSupportedException();
                        });
        }

        public Task<ReadStreamPage> ReadNext(CancellationToken cancellationToken = default(CancellationToken))
        {
            return _readNext(NextStreamVersion, cancellationToken);
        }
    }
}