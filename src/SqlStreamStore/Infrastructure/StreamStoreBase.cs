namespace SqlStreamStore.Infrastructure
{
    using System;
    using System.Threading;
    using System.Threading.Tasks;
    using EnsureThat;
    using SqlStreamStore.Streams;
    using SqlStreamStore;
    using SqlStreamStore.Logging;

    public abstract class StreamStoreBase : ReadonlyStreamStoreBase, IStreamStore
    {
        private readonly TaskQueue _taskQueue = new TaskQueue();

        protected StreamStoreBase(
            TimeSpan metadataMaxAgeCacheExpiry,
            int metadataMaxAgeCacheMaxSize,
            GetUtcNow getUtcNow,
            string logName)
            : base(metadataMaxAgeCacheExpiry, metadataMaxAgeCacheMaxSize, getUtcNow, logName)
        {}

        public Task AppendToStream(
            string streamId,
            int expectedVersion,
            NewStreamMessage[] messages,
            CancellationToken cancellationToken = default(CancellationToken))
        {
            Ensure.That(streamId, nameof(streamId)).IsNotNullOrWhiteSpace().DoesNotStartWith("$");
            Ensure.That(messages, nameof(messages)).IsNotNull();

            if(Logger.IsDebugEnabled())
            {
                Logger.DebugFormat("AppendToStream {streamId} with expected version {expectedVersion} and " +
                                   "{messageCount} messages.", streamId, expectedVersion, messages.Length);
            }

            if(messages.Length == 0 && expectedVersion >= 0) 
            {
                // If there is an expected version then nothing to do...
                return TaskHelpers.CompletedTask;
            }
            // ... expectedVersion.NoStream and ExpectedVesion.Any may create an empty stream though
            return AppendToStreamInternal(streamId, expectedVersion, messages, cancellationToken);
        }

        /// <summary>
        /// Hard deletes a stream and all of its messages. Deleting a stream will result in a '$stream-deleted'
        /// message being appended to the '$deleted' stream. See <see cref="Deleted.StreamDeleted" /> for the
        /// message structure.
        /// </summary>
        /// <param name="streamId">The stream Id to delete.</param>
        /// <param name="expectedVersion">The stream expected version. See <see cref="ExpectedVersion" /> for const values.</param>
        /// <param name="cancellationToken">The cancellation instruction.</param>
        /// <returns>
        /// A task representing the asynchronous operation.
        /// </returns>
        public Task DeleteStream(
            string streamId,
            int expectedVersion = ExpectedVersion.Any,
            CancellationToken cancellationToken = default(CancellationToken))
        {
            Ensure.That(streamId, nameof(streamId)).IsNotNullOrWhiteSpace().DoesNotStartWith("$");

            if (Logger.IsDebugEnabled())
            {
                Logger.DebugFormat("DeleteStream {streamId} with expected version {expectedVersion} and " +
                                   "{messageCount} messages." , streamId, expectedVersion);
            }

            return DeleteStreamInternal(streamId, expectedVersion, cancellationToken);
        }

        public Task DeleteMessage(
            string streamId,
            Guid messageId,
            CancellationToken cancellationToken = default(CancellationToken))
        {
            Ensure.That(streamId, nameof(streamId)).IsNotNullOrWhiteSpace().DoesNotStartWith("$");

            if (Logger.IsDebugEnabled())
            {
                Logger.DebugFormat("DeleteMessage {streamId} with messageId {messageId}", streamId, messageId);
            }

            return DeleteEventInternal(streamId, messageId, cancellationToken);
        }

        /// <summary>
        /// Sets the metadata for a stream.
        /// </summary>
        /// <param name="streamId">The stream Id to whose metadata is to be set.</param>
        /// <param name="expectedStreamMetadataVersion">The expected version number of the metadata stream to apply the metadata. Used for concurrency
        /// handling. Default value is <see cref="ExpectedVersion.Any" />. If specified and does not match
        /// current version then <see cref="WrongExpectedVersionException" /> will be thrown.</param>
        /// <param name="maxAge">The max age of the messages in the stream in seconds.</param>
        /// <param name="maxCount">The max count of messages in the stream.</param>
        /// <param name="metadataJson">Custom meta data to associate with the stream.</param>
        /// <param name="cancellationToken">The cancellation instruction.</param>
        /// <returns>
        /// A task representing the asynchronous operation.
        /// </returns>
        public Task SetStreamMetadata(
            string streamId,
            int expectedStreamMetadataVersion,
            int? maxAge = null,
            int? maxCount = null,
            string metadataJson = null,
            CancellationToken cancellationToken = default(CancellationToken))
        {
            Ensure.That(streamId, nameof(streamId)).IsNotNullOrWhiteSpace().DoesNotStartWith("$");
            Ensure.That(expectedStreamMetadataVersion, nameof(expectedStreamMetadataVersion)).IsGte(-2);

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

        public abstract Task<int> GetmessageCount(
            string streamId,
            CancellationToken cancellationToken = default(CancellationToken));

        protected override void PurgeExpiredMessage(StreamMessage streamMessage)
        {
            _taskQueue.Enqueue(ct => DeleteEventInternal(streamMessage.StreamId, streamMessage.MessageId, ct));
        }

        protected abstract Task AppendToStreamInternal(
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

        protected abstract Task SetStreamMetadataInternal(
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

        public virtual Task InitializeStore(
            bool ignoreErrors = false,
            CancellationToken cancellationToken = default(CancellationToken)) => Task.FromResult(0);
    }
}