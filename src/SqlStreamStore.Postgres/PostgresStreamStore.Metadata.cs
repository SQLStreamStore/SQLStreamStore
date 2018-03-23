namespace SqlStreamStore
{
    using System;
    using System.Threading;
    using System.Threading.Tasks;
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
            {
                await connection.OpenAsync(cancellationToken).NotOnCapturedContext();

                ReadStreamPage page;
                using(var transaction = connection.BeginTransaction())
                {
                    page = await ReadStreamInternal(
                        streamIdInfo.MetadataPosgresqlStreamId,
                        StreamVersion.End,
                        1,
                        ReadDirection.Backward,
                        true,
                        null,
                        transaction,
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
            var streamIdInfo = new StreamIdInfo(streamId);
            
            using(var connection = _createConnection())
            {
                await connection.OpenAsync(cancellationToken).NotOnCapturedContext();

                using(var transaction = connection.BeginTransaction())
                {
                    var json = SimpleJson.SerializeObject(new MetadataMessage
                    {
                        StreamId = streamId,
                        MaxAge = maxAge,
                        MaxCount = maxCount,
                        MetaJson = metadataJson
                    });
                    
                    var metadataMessage = new NewStreamMessage(Guid.NewGuid(), "$stream-metadata", json);

                    result = await AppendToStreamInternal(
                        streamIdInfo.PostgresqlStreamId,
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
}