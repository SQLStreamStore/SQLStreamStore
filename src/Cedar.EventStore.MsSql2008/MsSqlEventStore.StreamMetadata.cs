namespace Cedar.EventStore
{
    using System;
    using System.Data.SqlClient;
    using System.Threading;
    using System.Threading.Tasks;
    using Cedar.EventStore.Infrastructure;
    using Cedar.EventStore.Json;
    using Cedar.EventStore.Streams;

    public partial class MsSqlEventStore
    {
        protected override async Task<StreamMetadataResult> GetStreamMetadataInternal(
            string streamId,
            CancellationToken cancellationToken)
        {
            string metaStreamId = $"$${streamId}";

            var eventsPage = await ReadStreamBackwardsInternal(
                metaStreamId, StreamVersion.End, 1, cancellationToken);

            if(eventsPage.Status == PageReadStatus.StreamNotFound)
            {
                return new StreamMetadataResult(streamId, -1);
            }

            var metadataMessage = SimpleJson.DeserializeObject<MetadataMessage>(eventsPage.Events[0].JsonData);

            return new StreamMetadataResult(
                   streamId,
                   eventsPage.LastStreamVersion,
                   metadataMessage.MaxAge,
                   metadataMessage.MaxCount,
                   metadataMessage.MetaJson);
        }

        public override async Task SetStreamMetadataInternal(
            string streamId,
            int expectedStreamMetadataVersion,
            int? maxAge,
            int? maxCount,
            string metadataJson,
            CancellationToken cancellationToken)
        {
            using(var connection = _createConnection())
            {
                await connection.OpenAsync(cancellationToken);

                using(var transaction = connection.BeginTransaction())
                {
                    string metaStreamId = $"$${streamId}";

                    var metadataMessage = new MetadataMessage
                    {
                        StreamId = streamId,
                        MaxAge = maxAge,
                        MaxCount = maxCount,
                        MetaJson = metadataJson
                    };
                    var json = SimpleJson.SerializeObject(metadataMessage);
                    var newStreamEvent = new NewStreamEvent(Guid.NewGuid(), "$stream-metadata", json);

                    await AppendToStreamInternal(
                        connection,
                        transaction,
                        metaStreamId,
                        expectedStreamMetadataVersion,
                        new[] { newStreamEvent },
                        cancellationToken);

                    var streamIdInfo = new StreamIdInfo(streamId);

                    using(var command = new SqlCommand(_scripts.SetStreamMetadata, connection, transaction))
                    {
                        command.Parameters.AddWithValue("streamId", streamIdInfo.Hash);
                        command.Parameters.AddWithValue("maxAge", maxAge);
                        command.Parameters.AddWithValue("maxCount", maxCount);

                        await command
                            .ExecuteNonQueryAsync(cancellationToken)
                            .NotOnCapturedContext();
                    }

                    transaction.Commit();
                }
            }
        }
    }
}
