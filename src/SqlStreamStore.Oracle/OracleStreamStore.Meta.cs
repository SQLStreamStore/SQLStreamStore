namespace SqlStreamStore.Oracle
{
    using System.Threading;
    using System.Threading.Tasks;
    using SqlStreamStore.Infrastructure;
    using SqlStreamStore.Streams;
    using StreamStoreStore.Json;

    partial class OracleStreamStore
    {
        protected override async Task<StreamMetadataResult> GetStreamMetadataInternal(string streamId, CancellationToken cancellationToken)
        {
            var streamIdInfo = new StreamIdInfo(streamId);

            ReadStreamPage page = await ReadStreamInternal(
                streamIdInfo.MetaStreamId,
                StreamVersion.End,
                1,
                ReadDirection.Backward,
                true,
                null,
                cancellationToken);

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
                streamId,
                expectedStreamMetadataVersion,
                json);
            
            var newStreamMessage = new NewStreamMessage(messageId, "$stream-metadata", json);

            var metaStreamIdInfo = new StreamIdInfo(streamIdInfo.MetaStreamId.IdOriginal);
            
            using(var trans = await StartTransaction(cancellationToken))
            {
                var result = await AppendToStreamInternal(
                    trans,
                    metaStreamIdInfo,
                    expectedStreamMetadataVersion,
                    new[] { newStreamMessage },
                    cancellationToken);

                using(var cmd = _commandBuilder.SetMeta(trans, streamIdInfo, maxAge, maxCount))
                {
                    await cmd.ExecuteNonQueryAsync(cancellationToken).NotOnCapturedContext();
                }
                
                trans.Commit();
                
                return new SetStreamMetadataResult(result.CurrentVersion);
            }
        }

    }
}