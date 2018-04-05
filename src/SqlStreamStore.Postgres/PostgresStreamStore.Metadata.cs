namespace SqlStreamStore
{
    using System;
    using System.Threading;
    using System.Threading.Tasks;
    using Npgsql;
    using SqlStreamStore.Infrastructure;
    using SqlStreamStore.Streams;
    using StreamStoreStore.Json;

    partial class PostgresStreamStore
    {
        protected override async Task<StreamMetadataResult> GetStreamMetadataInternal(
            string streamId,
            CancellationToken cancellationToken)
        {
            var streamIdInfo = new StreamIdInfo(streamId);

            using(var connection = _createConnection())
            using(var transaction = await BeginTransaction(connection, cancellationToken))
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
            AppendResult result;

            var metadata = new MetadataMessage
            {
                StreamId = streamId,
                MaxAge = maxAge,
                MaxCount = maxCount,
                MetaJson = metadataJson
            };

            var streamIdInfo = new StreamIdInfo(streamId);

            using(var connection = _createConnection())
            using(var transaction = await BeginTransaction(connection, cancellationToken))
            {
                var json = SimpleJson.SerializeObject(metadata);

                var metadataMessage = new NewStreamMessage(Guid.NewGuid(), "$stream-metadata", json);

                (result, _) = await AppendToStreamInternal(
                    streamIdInfo.MetadataPosgresqlStreamId,
                    expectedStreamMetadataVersion,
                    new[] { metadataMessage },
                    transaction,
                    cancellationToken);

                await transaction.CommitAsync(cancellationToken).NotOnCapturedContext();
            }

            return new SetStreamMetadataResult(result.CurrentVersion);
        }
    }
}