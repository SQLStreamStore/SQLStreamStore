namespace SqlStreamStore
{
    using System;
    using System.Threading;
    using System.Threading.Tasks;
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
            using (var connection = OpenConnection())
            {
                page = ReadStreamInternal(
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

            var jsonPayload = page.Messages[0];
            var metadataMessage = await jsonPayload.GetJsonDataAs<MetadataMessage>(cancellationToken);
            return new StreamMetadataResult(
                streamId,
                page.LastStreamVersion,
                metadataMessage.MaxAge,
                metadataMessage.MaxCount,
                metadataMessage.MetaJson);
        }

        protected override Task<SetStreamMetadataResult> SetStreamMetadataInternal(
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
            var result = default((int nextExpectedVersion, AppendResult appendResult, bool messageExists));
            
            using(var connection = OpenConnection())
            using (var transaction = connection.BeginTransaction())
            using (var command = connection.CreateCommand())
            {
                command.Transaction = transaction;
                var message = new NewStreamMessage(
                    Guid.NewGuid(), 
                    "$stream-metadata",
                    metadataMessageJsonData); 
                
                if(expectedStreamMetadataVersion == ExpectedVersion.NoStream)
                {
                    result = AppendToStreamExpectedVersionNoStream(command,
                        streamIdInfo.MetadataSQLiteStreamId,
                        message,
                        cancellationToken);
                }
                else if(expectedStreamMetadataVersion == ExpectedVersion.Any)
                {
                    result = AppendToStreamExpectedVersionAny(command,
                        streamIdInfo.MetadataSQLiteStreamId,
                        message,
                        cancellationToken);
                }
                else if(expectedStreamMetadataVersion == ExpectedVersion.EmptyStream)
                {
                    result = AppendToStreamExpectedVersionEmptyStream(command,
                        streamIdInfo.MetadataSQLiteStreamId,
                        message,
                        cancellationToken);
                }
                else
                {
                    result = AppendToStreamExpectedVersion(command,
                        streamIdInfo.MetadataSQLiteStreamId,
                        expectedStreamMetadataVersion,
                        message,
                        cancellationToken);
                }

                command.CommandText = @"UPDATE streams
                                        SET max_age = @maxAge,
                                            max_count = @maxCount
                                        WHERE streams.id = @streamId";
                command.Parameters.Clear();
                command.Parameters.AddWithValue("@streamId", streamId);
                command.Parameters.AddWithValue("@maxAge", DBNull.Value);
                command.Parameters.AddWithValue("@maxCount", DBNull.Value);
                command.ExecuteNonQuery();
                
                transaction.Commit();
            }
            
            TryScavenge(streamIdInfo.SQLiteStreamId, cancellationToken);

            return Task.FromResult(new SetStreamMetadataResult(result.appendResult.CurrentVersion));
        }
   }
}