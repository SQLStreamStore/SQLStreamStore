namespace SqlStreamStore
{
    using System.Threading;
    using System.Threading.Tasks;
    using Npgsql;
    using SqlStreamStore.PgSqlScriptsV2;
    using SqlStreamStore.Streams;

    partial class PostgresStreamStoreV2
    {
        protected override async Task<StreamMetadataResult> GetStreamMetadataInternal(
            string streamId,
            CancellationToken cancellationToken)
        {
            var streamIdInfo = new StreamIdInfo(streamId);

            using(var connection = await OpenConnection(cancellationToken))
            using(var transaction = connection.BeginTransaction())
            {
                return await GetStreamMetadataInternal(streamIdInfo, transaction, cancellationToken);
            }
        }

        private async Task<StreamMetadataResult> GetStreamMetadataInternal(
            StreamIdInfo streamIdInfo,
            NpgsqlTransaction transaction,
            CancellationToken cancellationToken)
        {
            var page = await ReadStreamInternal(
                streamIdInfo.MetadataPosgresqlStreamId,
                StreamVersion.End,
                1,
                ReadDirection.Backward,
                true,
                null,
                transaction,
                cancellationToken);

            if(page.Status == PageReadStatus.StreamNotFound)
            {
                return new StreamMetadataResult(streamIdInfo.PostgresqlStreamId.IdOriginal, -1);
            }

            var metadataMessage = await page.Messages[0].GetJsonDataAs<MetadataMessage>(cancellationToken);

            return new StreamMetadataResult(
                streamIdInfo.PostgresqlStreamId.IdOriginal,
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
            int currentVersion;

            var metadata = new MetadataMessage
            {
                StreamId = streamId,
                MaxAge = maxAge,
                MaxCount = maxCount,
                MetaJson = metadataJson
            };

            var streamIdInfo = new StreamIdInfo(streamId);

            using(var connection = await OpenConnection(cancellationToken))
            using(var transaction = connection.BeginTransaction())
            using(var command = BuildFunctionCommand(
                _schema.SetStreamMetadata,
                transaction,
                Parameters.StreamId(streamIdInfo.PostgresqlStreamId),
                Parameters.MetadataStreamId(streamIdInfo.MetadataPosgresqlStreamId),
                Parameters.MetadataStreamIdOriginal(streamIdInfo.MetadataPosgresqlStreamId),
                Parameters.OptionalMaxAge(metadata.MaxAge),
                Parameters.OptionalMaxCount(metadata.MaxCount),
                Parameters.ExpectedVersion(expectedStreamMetadataVersion),
                Parameters.CreatedUtc(_settings.GetUtcNow?.Invoke()),
                Parameters.MetadataStreamMessage(
                    streamIdInfo.MetadataPosgresqlStreamId,
                    expectedStreamMetadataVersion,
                    metadata)))
            {
                currentVersion = (int) await command.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false);

                await transaction.CommitAsync(cancellationToken).ConfigureAwait(false);
            }

            await TryScavenge(streamIdInfo, cancellationToken);

            return new SetStreamMetadataResult(currentVersion);
        }
    }
}
