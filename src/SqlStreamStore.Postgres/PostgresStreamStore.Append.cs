namespace SqlStreamStore
{
    using System.Threading;
    using System.Threading.Tasks;
    using Npgsql;
    using SqlStreamStore.Infrastructure;
    using SqlStreamStore.PgSqlScripts;
    using SqlStreamStore.Streams;
    using StreamStoreStore.Json;

    partial class PostgresStreamStore
    {
        protected override async Task<AppendResult> AppendToStreamInternal(
            string streamId,
            int expectedVersion,
            NewStreamMessage[] messages,
            CancellationToken cancellationToken)
        {
            AppendResult result;
            MetadataMessage metadata;
            var streamIdInfo = new StreamIdInfo(streamId);

            using(var connection = _createConnection())
            using(var transaction = await BeginTransaction(connection, cancellationToken))
            {
                (result, metadata) = await AppendToStreamInternal(
                    streamIdInfo.PostgresqlStreamId,
                    expectedVersion,
                    messages,
                    transaction,
                    cancellationToken);

                await transaction.CommitAsync(cancellationToken).NotOnCapturedContext();
            }

            await TryScavenge(streamIdInfo, metadata?.MaxCount, cancellationToken).NotOnCapturedContext();

            return result;
        }

        private async Task<(AppendResult, MetadataMessage)> AppendToStreamInternal(
            PostgresqlStreamId streamId,
            int expectedVersion,
            NewStreamMessage[] messages,
            NpgsqlTransaction transaction,
            CancellationToken cancellationToken)
        {
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

                        var jsonData = reader.GetValue(2) as string;

                        var metadata = SimpleJson.DeserializeObject<MetadataMessage>(jsonData);

                        return (new AppendResult(reader.GetInt32(0), reader.GetInt64(1)), metadata);
                    }
                }
                catch(PostgresException ex) when(ex.IsWrongExpectedVersion())
                {
                    await transaction.RollbackAsync(cancellationToken).NotOnCapturedContext();

                    throw new WrongExpectedVersionException(
                        ErrorMessages.AppendFailedWrongExpectedVersion(streamId.IdOriginal, expectedVersion),
                        ex);
                }
            }
        }
    }
}