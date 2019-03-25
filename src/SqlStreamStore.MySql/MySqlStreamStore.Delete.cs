namespace SqlStreamStore
{
    using System;
    using System.Threading;
    using System.Threading.Tasks;
    using MySql.Data.MySqlClient;
    using SqlStreamStore.Infrastructure;
    using SqlStreamStore.MySqlScripts;
    using SqlStreamStore.Streams;

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

            using(var command = BuildStoredProcedureCall(
                _schema.DeleteStream,
                transaction,
                Parameters.StreamId(streamId),
                Parameters.ExpectedVersion(expectedVersion),
                Parameters.CreatedUtc(_settings.GetUtcNow?.Invoke()),
                Parameters.DeletedStreamId(),
                Parameters.DeletedStreamIdOriginal(),
                Parameters.DeletedMetadataStreamId(),
                Parameters.DeletedStreamMessageMessageId(deletedStreamMessage),
                Parameters.DeletedStreamMessageType(deletedStreamMessage),
                Parameters.DeletedStreamMessageJsonData(deletedStreamMessage)))
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

            using(var command = BuildStoredProcedureCall(
                _schema.DeleteStreamMessage,
                transaction,
                Parameters.StreamId(streamIdInfo.MySqlStreamId),
                Parameters.MessageId(eventId),
                Parameters.DeletedStreamId(),
                Parameters.DeletedStreamIdOriginal(),
                Parameters.DeletedMetadataStreamId(),
                Parameters.CreatedUtc(_settings.GetUtcNow?.Invoke()),
                Parameters.DeletedMessageMessageId(deletedMessageMessage),
                Parameters.DeletedMessageType(deletedMessageMessage),
                Parameters.DeletedMessageJsonData(deletedMessageMessage)))
            {
                await command.ExecuteNonQueryAsync(cancellationToken).NotOnCapturedContext();
            }
        }
    }
}