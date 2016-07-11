namespace Cedar.EventStore
{
    using System;
    using System.Threading;
    using System.Threading.Tasks;
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

        protected override async Task SetStreamMetadataInternal(
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

                    transaction.Commit();
                }
            }
        }
    }
}
