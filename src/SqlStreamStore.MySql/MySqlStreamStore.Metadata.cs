namespace SqlStreamStore
{
    using System.Threading;
    using System.Threading.Tasks;
    using MySqlConnector;
    using SqlStreamStore.Infrastructure;
    using SqlStreamStore.MySqlScripts;
    using SqlStreamStore.Streams;
    using StreamStoreStore.Json;

    partial class MySqlStreamStore
    {
        protected override async Task<StreamMetadataResult> GetStreamMetadataInternal(
            string streamId,
            CancellationToken cancellationToken)
        {
            var streamIdInfo = new StreamIdInfo(streamId);

            using(var connection = await OpenConnection(cancellationToken))
            using(var transaction = await connection.BeginTransactionAsync(cancellationToken).ConfigureAwait(false))
            {
                return await GetStreamMetadataInternal(streamIdInfo, transaction, cancellationToken);
            }
        }

        private async Task<StreamMetadataResult> GetStreamMetadataInternal(
            StreamIdInfo streamIdInfo,
            MySqlTransaction transaction,
            CancellationToken cancellationToken)
        {
            var page = await ReadStreamInternal(
                streamIdInfo.MetadataMySqlStreamId,
                StreamVersion.End,
                1,
                ReadDirection.Backward,
                true,
                null,
                transaction,
                cancellationToken);

            if(page.Status == PageReadStatus.StreamNotFound)
            {
                return new StreamMetadataResult(streamIdInfo.MySqlStreamId.IdOriginal, -1);
            }

            var metadataMessage = await page.Messages[0].GetJsonDataAs<MetadataMessage>(cancellationToken);

            return new StreamMetadataResult(
                streamIdInfo.MySqlStreamId.IdOriginal,
                page.LastStreamVersion,
                metadataMessage.MaxAge,
                metadataMessage.MaxCount,
                metadataMessage.MetaJson);
        }

        protected override async Task<SetStreamMetadataResult> SetStreamMetadataInternal(
            string streamId,
            int expectedStreamMetadataVersion,
            int? maxAge,
            int? maxCount,
            string metadataJson,
            CancellationToken cancellationToken)
        {
            var metadata = new MetadataMessage
            {
                StreamId = streamId,
                MaxAge = maxAge,
                MaxCount = maxCount,
                MetaJson = metadataJson
            };

            var metadataMessageJsonData = SimpleJson.SerializeObject(metadata);

            var streamIdInfo = new StreamIdInfo(streamId);

            var currentVersion = Parameters.CurrentVersion();

            try
            {
                using(var connection = await OpenConnection(cancellationToken))
                using(var transaction = await connection.BeginTransactionAsync(cancellationToken).ConfigureAwait(false))
                using(var command = BuildStoredProcedureCall(
                    _schema.SetStreamMetadata,
                    transaction,
                    Parameters.StreamId(streamIdInfo.MySqlStreamId),
                    Parameters.MetadataStreamId(streamIdInfo.MetadataMySqlStreamId),
                    Parameters.MetadataStreamIdOriginal(streamIdInfo.MetadataMySqlStreamId),
                    Parameters.OptionalMaxAge(metadata.MaxAge),
                    Parameters.OptionalMaxCount(metadata.MaxCount),
                    Parameters.ExpectedVersion(expectedStreamMetadataVersion),
                    Parameters.CreatedUtc(_settings.GetUtcNow?.Invoke()),
                    Parameters.MetadataMessageMessageId(
                        streamIdInfo.MetadataMySqlStreamId,
                        expectedStreamMetadataVersion,
                        metadataMessageJsonData),
                    Parameters.MetadataMessageType(),
                    Parameters.MetadataMessageJsonData(metadataMessageJsonData),
                    currentVersion,
                    Parameters.CurrentPosition()))
                {
                    await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);

                    await transaction.CommitAsync(cancellationToken).ConfigureAwait(false);
                }

                await TryScavenge(streamIdInfo, cancellationToken);

                return new SetStreamMetadataResult((int) currentVersion.Value);
            }
            catch(MySqlException ex) when(ex.IsWrongExpectedVersion())
            {
                throw new WrongExpectedVersionException(
                    ErrorMessages.AppendFailedWrongExpectedVersion(
                        streamIdInfo.MetadataMySqlStreamId.IdOriginal,
                        expectedStreamMetadataVersion),
                    streamIdInfo.MetadataMySqlStreamId.IdOriginal,
                    ExpectedVersion.Any,
                    ex);
            }
        }
    }
}
