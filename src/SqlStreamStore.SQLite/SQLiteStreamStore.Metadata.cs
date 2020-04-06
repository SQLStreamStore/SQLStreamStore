namespace SqlStreamStore
{
    using System;
    using System.Threading;
    using System.Threading.Tasks;
    using SqlStreamStore.Infrastructure;
    using SqlStreamStore.Streams;
    using StreamStoreStore.Json;

    public partial class SQLiteStreamStore
    {
        protected override Task<StreamMetadataResult> GetStreamMetadataInternal(
            string streamId, 
            CancellationToken cancellationToken)
        {
            var idInfo = new StreamIdInfo(streamId);

            ReadStreamPage page = ReadStreamInternal(
                idInfo.MetadataSQLiteStreamId.IdOriginal,
                StreamVersion.End,
                1,
                ReadDirection.Backward,
                true,
                null,
                cancellationToken);

            if (page.Status == PageReadStatus.StreamNotFound)
            {
                return Task.FromResult(new StreamMetadataResult(streamId, -1));
            }

            var jsonPayload = page.Messages[0];
            var json = jsonPayload.GetJsonData(CancellationToken.None).GetAwaiter().GetResult();
            var metadataMessage = SimpleJson.DeserializeObject<MetadataMessage>(json);
            return Task.FromResult(new StreamMetadataResult(
                streamId,
                page.LastStreamVersion,
                metadataMessage.MaxAge,
                metadataMessage.MaxCount,
                metadataMessage.MetaJson));
        }

        protected override Task<SetStreamMetadataResult> SetStreamMetadataInternal(
            string streamId, 
            int expectedStreamMetadataVersion, 
            int? maxAge, 
            int? maxCount, 
            string metadataJson, 
            CancellationToken cancellationToken)
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
                json
            );
            var message = new NewStreamMessage(
                messageId, 
                "$stream-metadata",
                json); 
            
            var idInfo = new StreamIdInfo(streamId);
            
            var result = AppendToStreamInternal(
                idInfo.MetadataSQLiteStreamId.IdOriginal,
                expectedStreamMetadataVersion,
                new[] { message },
                cancellationToken).GetAwaiter().GetResult();
            
            using (var connection = OpenConnection())
            using(var command = connection.CreateCommand())
            {
                command.CommandText = @"UPDATE streams
                                        SET max_age = @maxAge,
                                            max_count = @maxCount
                                        WHERE streams.id = @streamId";
                command.Parameters.AddWithValue("@streamId", idInfo.MetadataSQLiteStreamId.Id);
                command.Parameters.AddWithValue("@maxAge", maxAge ?? (object)DBNull.Value);
                command.Parameters.AddWithValue("@maxCount", maxCount ?? (object)DBNull.Value);
                command.ExecuteNonQuery(); // TODO: Check for records affected?
            }
            
            TryScavenge(idInfo.MetadataSQLiteStreamId, cancellationToken);

            return Task.FromResult(new SetStreamMetadataResult(result.CurrentVersion));
        }
   }
}