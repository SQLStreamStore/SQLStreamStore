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
            {
                await connection.OpenAsync(cancellationToken).NotOnCapturedContext();

                using(var transaction = connection.BeginTransaction())
                {
                    if(await DeleteStreamInternal(
                        streamIdInfo.PostgresqlStreamId,
                        expectedVersion,
                        transaction,
                        cancellationToken))
                    {
                        var streamDeletedEvent = Deleted.CreateStreamDeletedMessage(streamId);

                        await AppendToStreamInternal(
                            PostgresqlStreamId.Deleted,
                            ExpectedVersion.Any,
                            new[] { streamDeletedEvent },
                            transaction,
                            cancellationToken);
                    }

                    await DeleteStreamInternal(
                        streamIdInfo.PostgresqlStreamId,
                        expectedVersion,
                        transaction,
                        cancellationToken);

                    await transaction.CommitAsync(cancellationToken).NotOnCapturedContext();
                }
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
                catch(NpgsqlException ex)
                {
                    await transaction.RollbackAsync(cancellationToken).NotOnCapturedContext();

                    if(ex.Message.Contains("WrongExpectedVersion"))
                    {
                        throw new WrongExpectedVersionException(
                            ErrorMessages.DeleteStreamFailedWrongExpectedVersion(
                                streamId.IdOriginal,
                                expectedVersion),
                            ex);
                    }

                    throw;
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
            {
                await connection.OpenAsync(cancellationToken).NotOnCapturedContext();

                using(var transaction = connection.BeginTransaction())
                using(var command = BuildCommand(
                    _schema.DeleteStreamMessage,
                    transaction,
                    Parameters.StreamId(streamIdInfo.PostgresqlStreamId),
                    Parameters.MessageId(eventId),
                    Parameters.DeletedStreamId,
                    Parameters.DeletedStreamIdOriginal,
                    Parameters.CreatedUtc(_settings.GetUtcNow()),
                    Parameters.DeletedMessage(streamIdInfo.PostgresqlStreamId, eventId)))
                {
                    await command.ExecuteNonQueryAsync(cancellationToken).NotOnCapturedContext();
                    await transaction.CommitAsync(cancellationToken).NotOnCapturedContext();
                }
            }
        }
    }
}