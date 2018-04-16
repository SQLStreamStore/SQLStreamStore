namespace SqlStreamStore
{
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
            AppendResult result;
            int? maxCount;
            var streamIdInfo = new StreamIdInfo(streamId);

            using(var connection = _createConnection())
            using(var transaction = await BeginTransaction(connection, cancellationToken))
            using(var command = BuildCommand(
                _schema.AppendToStream,
                transaction,
                Parameters.StreamId(streamIdInfo.PostgresqlStreamId),
                Parameters.StreamIdOriginal(streamIdInfo.PostgresqlStreamId),
                Parameters.ExpectedVersion(expectedVersion),
                Parameters.CreatedUtc(_settings.GetUtcNow()),
                Parameters.NewStreamMessages(messages)))
            {
                try
                {
                    using(var reader = await command.ExecuteReaderAsync(cancellationToken).NotOnCapturedContext())
                    {
                        await reader.ReadAsync(cancellationToken).NotOnCapturedContext();

                        result = new AppendResult(reader.GetInt32(0), reader.GetInt64(1));
                        maxCount = reader.GetFieldValue<int?>(3);
                    }

                    await transaction.CommitAsync(cancellationToken).NotOnCapturedContext();
                }
                catch(PostgresException ex) when(ex.IsWrongExpectedVersion())
                {
                    await transaction.RollbackAsync(cancellationToken).NotOnCapturedContext();

                    throw new WrongExpectedVersionException(
                        ErrorMessages.AppendFailedWrongExpectedVersion(
                            streamIdInfo.PostgresqlStreamId.IdOriginal,
                            expectedVersion),
                        ex);
                }
            }

            await TryScavenge(streamIdInfo, maxCount, cancellationToken).NotOnCapturedContext();

            return result;
        }
    }
}