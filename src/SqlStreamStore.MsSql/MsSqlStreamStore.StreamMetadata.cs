namespace SqlStreamStore
{
    using System.Threading;
    using System.Threading.Tasks;
    using SqlStreamStore.Streams;
    using SqlStreamStore.Infrastructure;
    using StreamStoreStore.Json;

    public partial class MsSqlStreamStore
    {
        protected override async Task<StreamMetadataResult> GetStreamMetadataInternal(
            string streamId,
            CancellationToken cancellationToken)
        {
            var streamIdInfo = new StreamIdInfo(streamId);

            ReadStreamPage page;
            using (var connection = _createConnection())
            {
                await connection.OpenAsync(cancellationToken).ConfigureAwait(false);
                page = await ReadStreamInternal(
                    streamIdInfo.MetadataSqlStreamId,
                    StreamVersion.End,
                    1,
                    ReadDirection.Backward,
                    true,
                    null,
                    connection,
                    null,
                    cancellationToken);
            }

            if(page.Status == PageReadStatus.StreamNotFound)
            {
                return new StreamMetadataResult(streamId, -1);
            }

            var metadataMessage = await page.Messages[0].GetJsonDataAs<MetadataMessage>(cancellationToken);

            return new StreamMetadataResult(
                   streamId,
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
            MsSqlAppendResult result;
            using(var connection = _createConnection())
            {
                await connection.OpenAsync(cancellationToken).ConfigureAwait(false);

                using(var transaction = connection.BeginTransaction())
                {
                    var streamIdInfo = new StreamIdInfo(streamId);

                    var metadataMessage = new MetadataMessage
                    {
                        StreamId = streamId,
                        MaxAge = maxAge,
                        MaxCount = maxCount,
                        MetaJson = metadataJson
                    };
                    var json = SimpleJson.SerializeObject(metadataMessage);
                    var messageId = MetadataMessageIdGenerator.Create(
                            streamIdInfo.MetadataSqlStreamId.IdOriginal,
                            expectedStreamMetadataVersion,
                            json);
                    var message = new NewStreamMessage(messageId, "$stream-metadata", json);

                    result = await AppendToStreamInternal(
                        connection,
                        transaction,
                        streamIdInfo.MetadataSqlStreamId,
                        expectedStreamMetadataVersion,
                        new[] { message },
                        cancellationToken);

                    transaction.Commit();
                }
            }

            await CheckStreamMaxCount(streamId, maxCount, cancellationToken);

            return new SetStreamMetadataResult(result.CurrentVersion);
        }
    }
}
