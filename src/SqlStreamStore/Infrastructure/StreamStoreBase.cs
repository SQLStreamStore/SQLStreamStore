namespace SqlStreamStore.Infrastructure
{
    using System;
    using System.Threading;
    using System.Threading.Tasks;
    using SqlStreamStore.Streams;
    using SqlStreamStore;
    using SqlStreamStore.Imports.Ensure.That;
    using SqlStreamStore.Logging;

    /// <summary>
    ///     Represents a base implementation of a stream store. It's primary purpose is to handle 
    ///     common needs across all derived implementations such as guard clauses and logging.
    /// </summary>
    public abstract class StreamStoreBase : ReadonlyStreamStoreBase, IStreamStore
    {
        private readonly TaskQueue _taskQueue = new TaskQueue();

        /// <summary>
        ///     Initialized an new instance of a <see cref="StreamStoreBase"/>
        /// </summary>
        /// <param name="metadataMaxAgeCacheExpiry"></param>
        /// <param name="metadataMaxAgeCacheMaxSize"></param>
        /// <param name="getUtcNow"></param>
        /// <param name="logName"></param>
        protected StreamStoreBase(
            TimeSpan metadataMaxAgeCacheExpiry,
            int metadataMaxAgeCacheMaxSize,
            GetUtcNow getUtcNow,
            string logName)
            : base(metadataMaxAgeCacheExpiry, metadataMaxAgeCacheMaxSize, getUtcNow, logName)
        {}

        /// <summary>
        ///     Initialized an new instance of a <see cref="StreamStoreBase"/>
        /// </summary>
        /// <param name="getUtcNow"></param>
        /// <param name="logName"></param>
        protected StreamStoreBase(GetUtcNow getUtcNow, string logName)
            : base(getUtcNow, logName)
        {}

        /// <inheritdoc />
        public Task<AppendResult> AppendToStream(StreamId streamId, int expectedVersion, NewStreamMessage[] messages, CancellationToken cancellationToken = default)
        {
            Ensure.That(streamId.Value, nameof(streamId)).DoesNotStartWith("$");
            Ensure.That(messages, nameof(messages)).IsNotNull();

            if (Logger.IsDebugEnabled())
            {
                Logger.DebugFormat("AppendToStream {streamId} with expected version {expectedVersion} and " +
                                   "{messageCount} messages.", streamId, expectedVersion, messages.Length);
            }
            if (messages.Length == 0 && expectedVersion >= 0)
            {
                // If there is an expected version then nothing to do...
                return CreateAppendResultAtHeadPosition(expectedVersion, cancellationToken);
            }
            // ... expectedVersion.NoStream and ExpectedVersion.Any may create an empty stream though
            return AppendToStreamInternal(streamId, expectedVersion, messages, cancellationToken);
        }

        private async Task<AppendResult> CreateAppendResultAtHeadPosition(int expectedVersion, CancellationToken cancellationToken)
        {
            var position = await ReadHeadPosition(cancellationToken);
            return new AppendResult(expectedVersion, position);
        }

        /// <inheritdoc />
        public Task DeleteStream(
            StreamId streamId,
            int expectedVersion = ExpectedVersion.Any,
            CancellationToken cancellationToken = default)
        {
            Ensure.That(streamId.Value, nameof(streamId)).DoesNotStartWith("$");

            if (Logger.IsDebugEnabled())
            {
                Logger.DebugFormat("DeleteStream {streamId} with expected version {expectedVersion}.", streamId, expectedVersion);
            }

            return DeleteStreamInternal(streamId, expectedVersion, cancellationToken);
        }

        /// <inheritdoc />
        public Task DeleteMessage(
            StreamId streamId,
            Guid messageId,
            CancellationToken cancellationToken = default)
        {
            Ensure.That(streamId.Value, nameof(streamId)).DoesNotStartWith("$");

            if (Logger.IsDebugEnabled())
            {
                Logger.DebugFormat("DeleteMessage {streamId} with messageId {messageId}", streamId, messageId);
            }

            return DeleteEventInternal(streamId, messageId, cancellationToken);
        }

        /// <inheritdoc />
        public Task SetStreamMetadata(
            StreamId streamId,
            int expectedStreamMetadataVersion = ExpectedVersion.Any,
            int? maxAge = null,
            int? maxCount = null,
            string metadataJson = null,
            CancellationToken cancellationToken = default)
        {
            if(streamId == null) throw new ArgumentNullException(nameof(streamId));
            if(streamId.Value.StartsWith("$") && streamId.Value != Deleted.DeletedStreamId)
            {
                throw new ArgumentException("Must not start with '$'", nameof(streamId));
            }

            Ensure.That(expectedStreamMetadataVersion, nameof(expectedStreamMetadataVersion))
                .IsGte(ExpectedVersion.NoStream);

            if (Logger.IsDebugEnabled())
            {
                Logger.DebugFormat("SetStreamMetadata {streamId} with expected metadata version " +
                                   "{expectedStreamMetadataVersion}, max age {maxAge} and max count {maxCount}.",
                                   streamId, expectedStreamMetadataVersion, maxAge, maxCount);
            }

            return SetStreamMetadataInternal(
                streamId,
                expectedStreamMetadataVersion,
                maxAge,
                maxCount,
                metadataJson,
                cancellationToken);
        }

        /// <summary>
        ///     Queues a task to purge expired message.
        /// </summary>
        /// <param name="streamMessage"></param>
        protected override void PurgeExpiredMessage(StreamMessage streamMessage)
        {
            _taskQueue.Enqueue(ct => DeleteEventInternal(streamMessage.StreamId, streamMessage.MessageId, ct));
        }


#pragma warning disable CS1591 // Missing XML comment for publicly visible type or member

        protected abstract Task<AppendResult> AppendToStreamInternal(
            string streamId,
            int expectedVersion,
            NewStreamMessage[] messages,
            CancellationToken cancellationToken);

        protected abstract Task DeleteStreamInternal(
            string streamId,
            int expectedVersion,
            CancellationToken cancellationToken);

        protected abstract Task DeleteEventInternal(
            string streamId,
            Guid eventId,
            CancellationToken cancellationToken);

        protected abstract Task<SetStreamMetadataResult> SetStreamMetadataInternal(
           string streamId,
           int expectedStreamMetadataVersion,
           int? maxAge,
           int? maxCount,
           string metadataJson,
           CancellationToken cancellationToken);

        protected override void Dispose(bool disposing)
        {
            if(disposing)
            {
                _taskQueue.Dispose();
            }
            base.Dispose(disposing);
        }

#pragma warning restore CS1591 // Missing XML comment for publicly visible type or member

    }

}
