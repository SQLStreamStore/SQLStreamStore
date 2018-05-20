namespace SqlStreamStore.Streams
{
    using System;
    using System.Threading;
    using System.Threading.Tasks;

    /// <summary>
    ///     Represents t
    /// </summary>
    public struct StreamMessage
    {
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
            string jsonMetadata,
            string jsonData)
            : this(streamId,
                messageId,
                streamVersion,
                position,
                createdUtc,
                type,
                jsonMetadata, _ => Task.FromResult(jsonData))
        {}

        public StreamMessage(
            string streamId,
            Guid messageId,
            int streamVersion,
            long position,
            DateTime createdUtc,
            string type,
            string jsonMetadata,
            Func<CancellationToken, Task<string>> getJsonData)
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

        /// <summary>
        ///     Gets the Json Data of the message. If prefetch is enabled, this will be a fast operation. 
        /// </summary>
        /// <param name="cancellationToken">
        ///     The cancellation instruction.
        /// </param>
        /// <returns>
        ///     The Json Data of the message. If the message has been subsequently deleted since this 
        ///     StreamMessage was created, then it will return null.
        /// </returns>
        public Task<string> GetJsonData(CancellationToken cancellationToken = default)
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