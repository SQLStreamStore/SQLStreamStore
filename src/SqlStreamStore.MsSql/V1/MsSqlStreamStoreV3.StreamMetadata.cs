namespace SqlStreamStore.V1
{
    using System.Data;
    using System.Data.SqlClient;
    using System.Threading;
    using System.Threading.Tasks;
    using SqlStreamStore.V1.Infrastructure;
    using SqlStreamStore.V1.Streams;

    public partial class MsSqlStreamStoreV3
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
                (page, _) = await ReadStreamInternal(
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
                var streamIdInfo = new StreamIdInfo(streamId);

                await connection.OpenAsync(cancellationToken).NotOnCapturedContext();

                using(var transaction = connection.BeginTransaction())
                {
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

                    result = await AppendToStreamInternal(
                        connection,
                        transaction,
                        streamIdInfo.MetadataSqlStreamId,
                        expectedStreamMetadataVersion,
                        new[] { newStreamMessage },
                        cancellationToken);

                    using(var command = new SqlCommand(_scripts.SetStreamMetadata, connection, transaction))
                    {
                        command.Parameters.Add(new SqlParameter("streamId", SqlDbType.Char, 42) { Value = streamIdInfo.SqlStreamId.Id });
                        command.Parameters.AddWithValue("streamIdOriginal", streamIdInfo.SqlStreamId.IdOriginal);
                        command.Parameters.Add("maxAge", SqlDbType.Int);
                        command.Parameters["maxAge"].Value = maxAge ?? -1;
                        command.Parameters.Add("maxCount", SqlDbType.Int);
                        command.Parameters["maxCount"].Value = maxCount ?? -1;
                        await command.ExecuteNonQueryAsync(cancellationToken);
                    }

                    transaction.Commit();
                }
            }

            await CheckStreamMaxCount(streamId, maxCount, cancellationToken);

            return new SetStreamMetadataResult(result.CurrentVersion);
        }
    }
}
