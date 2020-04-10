namespace SqlStreamStore
{
    using System;
    using System.Linq;
    using System.Threading;
    using System.Threading.Tasks;
    using SqlStreamStore.Infrastructure;
    using SqlStreamStore.Streams;
    using StreamStoreStore.Json;

    public partial class SqliteStreamStore
    {
        protected override async Task<StreamMetadataResult> GetStreamMetadataInternal(string streamId, CancellationToken cancellationToken)
        {
            GuardAgainstDisposed();
            cancellationToken.ThrowIfCancellationRequested();
            
            var idInfo = new StreamIdInfo(streamId);

            var page = await ReadStreamBackwardsInternal(
                idInfo.MetadataSqlStreamId.IdOriginal,
                StreamVersion.End,
                1,
                true,
                null,
                cancellationToken
            ).NotOnCapturedContext();

            if(page.Status == PageReadStatus.StreamNotFound)
            {
                return new StreamMetadataResult(streamId, -1);
            }

            var payload = page.Messages.FirstOrDefault(); //TODO: What to do when page.Messages.Count() == 0?
            
            var json = await payload.GetJsonData(cancellationToken).NotOnCapturedContext();
            var msg = SimpleJson.DeserializeObject<MetadataMessage>(json);
            return new StreamMetadataResult(streamId, page.LastStreamVersion, msg.MaxAge, msg.MaxCount, msg.MetaJson);
        }

        protected override async Task<SetStreamMetadataResult> SetStreamMetadataInternal(
            string streamId,
            int expectedStreamMetadataVersion,
            int? maxAge,
            int? maxCount,
            string metadataJson,
            CancellationToken cancellationToken)
        {
            GuardAgainstDisposed();
            cancellationToken.ThrowIfCancellationRequested();
            
            var idInfo = new StreamIdInfo(streamId);
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
           
            
            var result = await AppendToStreamInternal(
                idInfo.MetadataSqlStreamId.IdOriginal,
                expectedStreamMetadataVersion,
                new[] { message },
                cancellationToken).NotOnCapturedContext();
            
            using (var connection = OpenConnection())
            using(var command = connection.CreateCommand())
            {
                command.CommandText = @"REPLACE INTO streams(id, id_original, max_age, max_count)
                                        VALUES(@id, @idOriginal, @maxAge, @maxCount)";
                command.Parameters.Clear();
                command.Parameters.AddWithValue("@id", idInfo.MetadataSqlStreamId.Id);
                command.Parameters.AddWithValue("@idOriginal", idInfo.MetadataSqlStreamId.IdOriginal);
                command.Parameters.AddWithValue("@maxAge", maxAge ?? (object)DBNull.Value);
                command.Parameters.AddWithValue("@maxCount", maxCount ?? (object)DBNull.Value);
                command.ExecuteNonQuery(); // TODO: Check for records affected?
            }
            
            await TryScavengeAsync(idInfo.MetadataSqlStreamId, cancellationToken).NotOnCapturedContext();

            return new SetStreamMetadataResult(result.CurrentVersion);
        }
    }
}