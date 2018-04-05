namespace SqlStreamStore
{
    using System;
    using System.Threading;
    using System.Threading.Tasks;
    using Npgsql;
    using SqlStreamStore.Infrastructure;
    using SqlStreamStore.PgSqlScripts;
    using SqlStreamStore.Streams;

    public partial class PostgresStreamStore
    {
        protected override async Task DeleteStreamInternal(
            string streamId,
            int expectedVersion,
            CancellationToken cancellationToken)
        {
            var streamIdInfo = new StreamIdInfo(streamId);

            using(var connection = _createConnection())
            using(var transaction = await BeginTransaction(connection, cancellationToken))
            {
                if(await DeleteStreamInternal(
                    streamIdInfo.PostgresqlStreamId,
                    expectedVersion,
                    transaction,
                    cancellationToken))
                {
                    await AppendToStreamInternal(
                        PostgresqlStreamId.Deleted,
                        ExpectedVersion.Any,
                        new[]
                        {
                            Deleted.CreateStreamDeletedMessage(streamIdInfo.PostgresqlStreamId.IdOriginal)
                        },
                        transaction,
                        cancellationToken);
                }

                if(await DeleteStreamInternal(
                    streamIdInfo.MetadataPosgresqlStreamId,
                    ExpectedVersion.Any,
                    transaction,
                    cancellationToken))
                {
                    await AppendToStreamInternal(
                        PostgresqlStreamId.Deleted,
                        ExpectedVersion.Any,
                        new[]
                        {
                            Deleted.CreateStreamDeletedMessage(streamIdInfo.MetadataPosgresqlStreamId.IdOriginal)
                        },
                        transaction,
                        cancellationToken);
                }

                await transaction.CommitAsync(cancellationToken).NotOnCapturedContext();
            }
        }

        private async Task<bool> DeleteStreamInternal(
            PostgresqlStreamId streamId,
            int expectedVersion,
            NpgsqlTransaction transaction,
            CancellationToken cancellationToken)
        {
            using(var command = BuildCommand(
                _schema.DeleteStream,
                transaction,
                Parameters.StreamId(streamId),
                Parameters.ExpectedVersion(expectedVersion)))
            {
                try
                {
                    var result = await command.ExecuteScalarAsync(cancellationToken).NotOnCapturedContext();

                    return (int) result > 0;
                }
                catch(PostgresException ex) when(ex.IsWrongExpectedVersion())
                {
                    await transaction.RollbackAsync(cancellationToken).NotOnCapturedContext();

                    throw new WrongExpectedVersionException(
                        ErrorMessages.DeleteStreamFailedWrongExpectedVersion(
                            streamId.IdOriginal,
                            expectedVersion),
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

            using(var connection = _createConnection())
            using(var transaction = await BeginTransaction(connection, cancellationToken))
            {
                await DeleteEventsInternal(streamIdInfo, new[] { eventId }, transaction, cancellationToken);

                await transaction.CommitAsync(cancellationToken).NotOnCapturedContext();
            }
        }

        private async Task DeleteEventsInternal(
            StreamIdInfo streamIdInfo,
            Guid[] eventIds,
            NpgsqlTransaction transaction,
            CancellationToken cancellationToken)
        {
            using(var command = BuildCommand(
                _schema.DeleteStreamMessages,
                transaction,
                Parameters.StreamId(streamIdInfo.PostgresqlStreamId),
                Parameters.MessageIds(eventIds),
                Parameters.DeletedStreamId,
                Parameters.DeletedStreamIdOriginal,
                Parameters.CreatedUtc(_settings.GetUtcNow()),
                Parameters.DeletedMessages(streamIdInfo.PostgresqlStreamId, eventIds)))
            {
                await command.ExecuteNonQueryAsync(cancellationToken).NotOnCapturedContext();
            }
        }
    }
}