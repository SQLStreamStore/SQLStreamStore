namespace SqlStreamStore
{
    using System;
    using System.Threading;
    using System.Threading.Tasks;
    using SqlStreamStore.Streams;
    using SqlStreamStore.Infrastructure;
    using StreamStoreStore.Json;

    public partial class MsSqlEventStore
    {
        protected override async Task<StreamMetadataResult> GetStreamMetadataInternal(
            string streamId,
            CancellationToken cancellationToken)
        {
            var streamIdInfo = new StreamIdInfo(streamId);

            StreamEventsPage eventsPage;
            using (var connection = _createConnection())
            {
                await connection.OpenAsync(cancellationToken).NotOnCapturedContext();
                eventsPage = await ReadStreamInternal(streamIdInfo.MetadataSqlStreamId, StreamVersion.End, 1, ReadDirection.Backward, connection, cancellationToken);
            }

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
                    var streamIdInfo = new StreamIdInfo(streamId);

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
                        streamIdInfo.MetadataSqlStreamId,
                        expectedStreamMetadataVersion,
                        new[] { newStreamEvent },
                        cancellationToken);

                    transaction.Commit();
                }
            }

            await CheckStreamMaxCount(streamId, maxCount, cancellationToken);
        }
    }
}
