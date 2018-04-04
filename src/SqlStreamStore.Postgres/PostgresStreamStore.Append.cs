namespace SqlStreamStore
{
    using System.Data;
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
            {
                await connection.OpenAsync(cancellationToken).NotOnCapturedContext();
                using(var transaction = connection.BeginTransaction(IsolationLevel.ReadCommitted))
                {
                    (result, metadata) = await AppendToStreamInternal(
                        streamIdInfo.PostgresqlStreamId,
                        expectedVersion,
                        messages,
                        transaction,
                        cancellationToken);
                    
                    await transaction.CommitAsync(cancellationToken).NotOnCapturedContext();
                }
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