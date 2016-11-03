namespace SqlStreamStore.Streams
{
    using System;
    using System.Threading;
    using System.Threading.Tasks;

    public struct StreamMessage
    {
        public static readonly StreamMessage[] EmptyArray = new StreamMessage[0];

        public readonly long Position;
        public readonly DateTime CreatedUtc;
        public readonly Guid MessageId;
        public readonly string JsonMetadata;
        public readonly int StreamVersion;
        public readonly string StreamId;
        public readonly string Type;
        private readonly Func<CancellationToken, Task<string>> _getJsonData;

        public StreamMessage(
            string streamId,
            Guid messageId,
            int streamVersion,
            long position,
            DateTime createdUtc,
            string type,
            string jsonData,
            string jsonMetadata)
            : this(streamId,
                messageId,
                streamVersion,
                position,
                createdUtc,
                type,
                _ => Task.FromResult(jsonData),
                jsonMetadata)
        {}

        public StreamMessage(
            string streamId,
            Guid messageId,
            int streamVersion,
            long position,
            DateTime createdUtc,
            string type,
            Func<CancellationToken, Task<string>> getJsonData,
            string jsonMetadata)
        {
            MessageId = messageId;
            StreamId = streamId;
            StreamVersion = streamVersion;
            Position = position;
            CreatedUtc = createdUtc;
            Type = type;
            _getJsonData = getJsonData;
            JsonMetadata = jsonMetadata;
        }

        public Task<string> GetJsonData(CancellationToken cancellationToken = default(CancellationToken))
        {
            return _getJsonData(cancellationToken);
        }

        /// <summary>
        /// Returns a string that represents the current object.
        /// </summary>
        /// <returns>
        /// A string that represents the current object.
        /// </returns>
        /// <filterpriority>2</filterpriority>
        public override string ToString()
        {
            return $"MessageId={MessageId} StreamId={StreamId} StreamVersion={StreamVersion} Position={Position} Type={Type}";
        }
    }
}