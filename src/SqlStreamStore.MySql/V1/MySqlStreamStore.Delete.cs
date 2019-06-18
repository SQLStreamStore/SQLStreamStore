namespace SqlStreamStore.V1
{
    using System;
    using System.Threading;
    using System.Threading.Tasks;
    using MySql.Data.MySqlClient;
    using SqlStreamStore.V1.Infrastructure;
    using SqlStreamStore.V1.MySqlScripts;
    using SqlStreamStore.V1.Streams;

    partial class MySqlStreamStore
    {
        protected override async Task DeleteStreamInternal(
            string streamId,
            int expectedVersion,
            CancellationToken cancellationToken)
        {
            var streamIdInfo = new StreamIdInfo(streamId);

            using(var connection = await OpenConnection(cancellationToken))
            using(var transaction = connection.BeginTransaction())
            {
                await DeleteStreamInternal(
                    streamIdInfo.MySqlStreamId,
                    expectedVersion,
                    transaction,
                    cancellationToken);

                await DeleteStreamInternal(
                    streamIdInfo.MetadataMySqlStreamId,
                    ExpectedVersion.Any,
                    transaction,
                    cancellationToken);

                await transaction.CommitAsync(cancellationToken).NotOnCapturedContext();
            }
        }

        private async Task DeleteStreamInternal(
            MySqlStreamId streamId,
            int expectedVersion,
            MySqlTransaction transaction,
            CancellationToken cancellationToken)
        {
            var deletedStreamMessage = Deleted.CreateStreamDeletedMessage(streamId.IdOriginal);

            var deletedStreamId = Parameters.DeletedStreamId();
            var deletedStreamIdOriginal = Parameters.DeletedStreamIdOriginal();
            var deletedMetadataStreamId = Parameters.DeletedMetadataStreamId();
            var deletedStreamMessageMessageId = Parameters.DeletedStreamMessageMessageId(deletedStreamMessage);
            var deletedStreamMessageType = Parameters.DeletedStreamMessageType(deletedStreamMessage);
            var deletedStreamMessageJsonData = Parameters.DeletedStreamMessageJsonData(deletedStreamMessage);

            using(var command = BuildStoredProcedureCall(
                _schema.DeleteStream,
                transaction,
                Parameters.StreamId(streamId),
                Parameters.ExpectedVersion(expectedVersion),
                Parameters.CreatedUtc(_settings.GetUtcNow?.Invoke()),
                Parameters.DeletionTrackingDisabled(_settings.DisableDeletionTracking),
                _settings.DisableDeletionTracking ? deletedStreamId.Empty() : deletedStreamId,
                _settings.DisableDeletionTracking ? deletedStreamIdOriginal.Empty() : deletedStreamIdOriginal,
                _settings.DisableDeletionTracking ? deletedMetadataStreamId.Empty() : deletedMetadataStreamId,
                _settings.DisableDeletionTracking
                    ? deletedStreamMessageMessageId.Empty()
                    : deletedStreamMessageMessageId,
                _settings.DisableDeletionTracking ? deletedStreamMessageType.Empty() : deletedStreamMessageType,
                _settings.DisableDeletionTracking
                    ? deletedStreamMessageJsonData.Empty()
                    : deletedStreamMessageJsonData))
            {
                try
                {
                    await command.ExecuteNonQueryAsync(cancellationToken).NotOnCapturedContext();
                }
                catch(MySqlException ex) when(ex.IsWrongExpectedVersion())
                {
                    await transaction.RollbackAsync(cancellationToken).NotOnCapturedContext();

                    throw new WrongExpectedVersionException(
                        ErrorMessages.DeleteStreamFailedWrongExpectedVersion(streamId.IdOriginal, expectedVersion),
                        streamId.IdOriginal,
                        expectedVersion,
                        ex);
                }
            }
        }

        protected override async Task DeleteEventInternal(
            string streamId,
            Guid eventId,
            CancellationToken cancellationToken)
        {
            var streamIdInfo = new StreamIdInfo(streamId);

            using(var connection = await OpenConnection(cancellationToken))
            using(var transaction = connection.BeginTransaction())
            {
                await DeleteEventInternal(streamIdInfo, eventId, transaction, cancellationToken);

                await transaction.CommitAsync(cancellationToken).NotOnCapturedContext();
            }
        }

        private async Task DeleteEventInternal(
            StreamIdInfo streamIdInfo,
            Guid eventId,
            MySqlTransaction transaction,
            CancellationToken cancellationToken)
        {
            var deletedMessageMessage = Deleted.CreateMessageDeletedMessage(
                streamIdInfo.MySqlStreamId.IdOriginal,
                eventId);

            var deletedStreamId = Parameters.DeletedStreamId();
            var deletedStreamIdOriginal = Parameters.DeletedStreamIdOriginal();
            var deletedMetadataStreamId = Parameters.DeletedMetadataStreamId();
            var deletedMessageMessageId = Parameters.DeletedMessageMessageId(deletedMessageMessage);
            var deletedMessageType = Parameters.DeletedMessageType(deletedMessageMessage);
            var deletedMessageJsonData = Parameters.DeletedMessageJsonData(deletedMessageMessage);

            using(var command = BuildStoredProcedureCall(
                _schema.DeleteStreamMessage,
                transaction,
                Parameters.StreamId(streamIdInfo.MySqlStreamId),
                Parameters.MessageId(eventId),
                Parameters.CreatedUtc(_settings.GetUtcNow?.Invoke()),
                Parameters.DeletionTrackingDisabled(_settings.DisableDeletionTracking),
                _settings.DisableDeletionTracking ? deletedStreamId.Empty() : deletedStreamId,
                _settings.DisableDeletionTracking ? deletedStreamIdOriginal.Empty() : deletedStreamIdOriginal,
                _settings.DisableDeletionTracking ? deletedMetadataStreamId.Empty() : deletedMetadataStreamId,
                _settings.DisableDeletionTracking ? deletedMessageMessageId.Empty() : deletedMessageMessageId,
                _settings.DisableDeletionTracking ? deletedMessageType.Empty() : deletedMessageType,
                _settings.DisableDeletionTracking ? deletedMessageJsonData.Empty() : deletedMessageJsonData))
            {
                await command.ExecuteNonQueryAsync(cancellationToken).NotOnCapturedContext();
            }
        }
    }
}