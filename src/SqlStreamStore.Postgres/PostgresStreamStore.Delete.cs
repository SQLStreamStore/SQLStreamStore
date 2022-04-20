namespace SqlStreamStore
{
    using System;
    using System.Threading;
    using System.Threading.Tasks;
    using Npgsql;
    using SqlStreamStore.PgSqlScriptsV2;
    using SqlStreamStore.Streams;

    public partial class PostgresStreamStore
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
                    streamIdInfo.PostgresqlStreamId,
                    expectedVersion,
                    transaction,
                    cancellationToken);

                await DeleteStreamInternal(
                    streamIdInfo.MetadataPosgresqlStreamId,
                    ExpectedVersion.Any,
                    transaction,
                    cancellationToken);

                await transaction.CommitAsync(cancellationToken).ConfigureAwait(false);
            }
        }

        private async Task DeleteStreamInternal(
            PostgresqlStreamId streamId,
            int expectedVersion,
            NpgsqlTransaction transaction,
            CancellationToken cancellationToken)
        {
            using(var command = BuildFunctionCommand(
                _schema.DeleteStream,
                transaction,
                Parameters.StreamId(streamId),
                Parameters.ExpectedVersion(expectedVersion),
                Parameters.CreatedUtc(_settings.GetUtcNow?.Invoke()),
                Parameters.DeletionTrackingDisabled(_settings.DisableDeletionTracking),
                _settings.DisableDeletionTracking ? Parameters.Empty() : Parameters.DeletedStreamId,
                _settings.DisableDeletionTracking ? Parameters.Empty() : Parameters.DeletedStreamIdOriginal,
                _settings.DisableDeletionTracking ? Parameters.Empty() : Parameters.DeletedStreamMessage(streamId)))
            {
                try
                {
                    await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
                }
                catch(PostgresException ex) when(ex.IsWrongExpectedVersion())
                {
                    await transaction.RollbackAsync(cancellationToken).ConfigureAwait(false);

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
                await DeleteEventsInternal(streamIdInfo, new[] { eventId }, transaction, cancellationToken);

                await transaction.CommitAsync(cancellationToken).ConfigureAwait(false);
            }
        }

        private async Task DeleteEventsInternal(
            StreamIdInfo streamIdInfo,
            Guid[] eventIds,
            NpgsqlTransaction transaction,
            CancellationToken cancellationToken)
        {
            using(var command = BuildFunctionCommand(
                _schema.DeleteStreamMessages,
                transaction,
                Parameters.StreamId(streamIdInfo.PostgresqlStreamId),
                Parameters.MessageIds(eventIds),
                Parameters.DeletionTrackingDisabled(_settings.DisableDeletionTracking),
                _settings.DisableDeletionTracking ? Parameters.Empty() : Parameters.DeletedStreamId,
                _settings.DisableDeletionTracking ? Parameters.Empty() : Parameters.DeletedStreamIdOriginal,
                Parameters.CreatedUtc(_settings.GetUtcNow?.Invoke()),
                _settings.DisableDeletionTracking
                    ? Parameters.Empty()
                    : Parameters.DeletedMessages(streamIdInfo.PostgresqlStreamId, eventIds)))
            {
                await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
            }
        }
    }
}
