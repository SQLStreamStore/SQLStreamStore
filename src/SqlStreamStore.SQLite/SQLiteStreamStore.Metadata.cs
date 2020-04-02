namespace SqlStreamStore
{
    using System.Threading;
    using System.Threading.Tasks;
    using SqlStreamStore.Infrastructure;
    using SqlStreamStore.Streams;
    using StreamStoreStore.Json;

    public partial class SQLiteStreamStore
    {
        protected override async Task<StreamMetadataResult> GetStreamMetadataInternal(
            string streamId, 
            CancellationToken cancellationToken)
        {
            var streamIdInfo = new StreamIdInfo(streamId);

            ReadStreamPage page;
            using (var connection = _createConnection())
            {
                await connection.OpenAsync(cancellationToken).NotOnCapturedContext();
                page = await ReadStreamInternal(
                    streamIdInfo.MetadataSQLiteStreamId,
                    StreamVersion.End,
                    1,
                    ReadDirection.Backward,
                    true,
                    null,
                    connection,
                    null,
                    cancellationToken);
            }

            if (page.Status == PageReadStatus.StreamNotFound)
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
            SQLiteAppendResult result;
            using(var connection = _createConnection())
            {
                await connection.OpenAsync(cancellationToken).NotOnCapturedContext();

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
                            streamIdInfo.MetadataSQLiteStreamId.IdOriginal,
                            expectedStreamMetadataVersion,
                            json);
                    var message = new NewStreamMessage(messageId, "$stream-metadata", json);

                    result = await AppendToStreamInternal(
                        connection,
                        transaction,
                        streamIdInfo.MetadataSQLiteStreamId,
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