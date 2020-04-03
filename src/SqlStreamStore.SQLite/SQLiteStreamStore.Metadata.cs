namespace SqlStreamStore
{
    using System;
    using System.Threading;
    using System.Threading.Tasks;
    using SqlStreamStore.Streams;

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

            var metadataMessage = await page.Messages[0].GetJsonDataAs<MetadataMessage>(cancellationToken);
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
            var metadataMessage = new MetadataMessage
            {
                StreamId = streamId,
                MaxAge = maxAge,
                MaxCount = maxCount,
                MetaJson = metadataJson
            };

            var streamIdInfo = new StreamIdInfo(streamId);
            var currentVersion = default(int?);

            using(var connection = OpenConnection())
            using (var transaction = connection.BeginTransaction())
            using (var command = connection.CreateCommand())
            {
                command.Transaction = transaction;
                
                if(expectedStreamMetadataVersion == ExpectedVersion.NoStream)
                {
                    AppendToStreamExpectedVersionNoStream(command,
                        streamIdInfo.MetadataSQLiteStreamId,
                        null,
                        cancellationToken);
                }
                else if(expectedStreamMetadataVersion == ExpectedVersion.Any)
                {
                    AppendToStreamExpectedVersionAny(command,
                        streamIdInfo.MetadataSQLiteStreamId,
                        null,
                        cancellationToken);
                }
                else if(expectedStreamMetadataVersion == ExpectedVersion.EmptyStream)
                {
                    AppendToStreamExpectedVersionEmptyStream(command,
                        streamIdInfo.MetadataSQLiteStreamId,
                        null,
                        cancellationToken);
                }
                else
                {
                    AppendToStreamExpectedVersion(command,
                        streamIdInfo.MetadataSQLiteStreamId,
                        expectedStreamMetadataVersion,
                        null,
                        cancellationToken);
                }

                command.CommandText = @"UPDATE streams
                                        SET max_age = @maxAge,
                                            max_count = @maxCount
                                        WHERE streams.id = @streamId";
                command.Parameters.Clear();
                command.Parameters.AddWithValue("@maxAge", DBNull.Value);
                command.Parameters.AddWithValue("@maxCount", DBNull.Value);
                command.ExecuteNonQuery();
                
                transaction.Commit();
            }
            
            TryScavenge(streamIdInfo, cancellationToken);

            return Task.FromResult(new SetStreamMetadataResult(currentVersion.Value));
        }
   }
}