namespace SqlStreamStore
{
    using System;
    using System.Data;
    using System.Threading;
    using System.Threading.Tasks;
    using Npgsql;
    using SqlStreamStore.Infrastructure;
    using SqlStreamStore.PgSqlScripts;
    using SqlStreamStore.Streams;

    partial class PostgresStreamStore
    {
        protected override async Task<AppendResult> AppendToStreamInternal(
            string streamId,
            int expectedVersion,
            NewStreamMessage[] messages,
            CancellationToken cancellationToken)
        {
            var streamIdInfo = new StreamIdInfo(streamId);

            using(var connection = _createConnection())
            {
                await connection.OpenAsync(cancellationToken).NotOnCapturedContext();
                using(var transaction = connection.BeginTransaction(IsolationLevel.ReadCommitted))
                {
                    var result = await AppendToStreamInternal(
                        streamIdInfo.PostgresqlStreamId,
                        expectedVersion,
                        messages,
                        transaction,
                        cancellationToken);

                    await transaction.CommitAsync(cancellationToken).NotOnCapturedContext();

                    return result;
                }
            }
        }

        private async Task<AppendResult> AppendToStreamInternal(
            PostgresqlStreamId streamId,
            int expectedVersion,
            NewStreamMessage[] messages,
            NpgsqlTransaction transaction,
            CancellationToken cancellationToken)
        {
            transaction.Connection.MapComposite<PostgresNewStreamMessage>(_schema.NewStreamMessage);

            using(var command = BuildCommand(
                _schema.AppendToStream,
                transaction,
                Parameters.StreamId(streamId),
                Parameters.StreamIdOriginal(streamId),
                Parameters.MetadataStreamId(new StreamIdInfo(streamId.IdOriginal).MetadataPosgresqlStreamId), // YUCK!
                Parameters.ExpectedVersion(expectedVersion),
                Parameters.CreatedUtc(_settings.GetUtcNow()),
                Parameters.NewStreamMessages(messages)))
            {
                try
                {
                    using(var reader = await command.ExecuteReaderAsync(cancellationToken).NotOnCapturedContext())
                    {
                        await reader.ReadAsync(cancellationToken).NotOnCapturedContext();

                        return new AppendResult(reader.GetInt32(0), reader.GetInt64(1));
                    }
                }
                catch(NpgsqlException ex) when(ex.IsWrongExpectedVersion())
                {
                    await transaction.RollbackAsync(cancellationToken).NotOnCapturedContext();
                    
                    throw new WrongExpectedVersionException(
                        ErrorMessages.AppendFailedWrongExpectedVersion(streamId.IdOriginal, expectedVersion),
                        ex);
                }
            }
        }

        private async Task<int> GetStreamVersionOfMessageId(
            PostgresqlStreamId streamId,
            Guid messageId,
            NpgsqlTransaction transaction,
            CancellationToken cancellationToken)
        {
            using(var command = BuildCommand(
                _schema.ReadStreamVersionOfMessageId,
                transaction,
                Parameters.StreamId(streamId),
                Parameters.MessageId(messageId)))
            {
                var result = await command.ExecuteScalarAsync(cancellationToken).NotOnCapturedContext();

                return (int) result;
            }
        }
    }
}