namespace SqlStreamStore
{
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
            ).ConfigureAwait(false);

            if(page.Status == PageReadStatus.StreamNotFound)
            {
                return new StreamMetadataResult(streamId, -1);
            }

            var payload = page.Messages.FirstOrDefault(); //TODO: What to do when page.Messages.Count() == 0?

            var json = await payload.GetJsonData(cancellationToken).ConfigureAwait(false);
            var msg = SimpleJson.DeserializeObject<MetadataMessage>(json);
            return new StreamMetadataResult(streamId, page.LastStreamVersion, msg.MaxAge, msg.MaxCount, msg.MetaJson);
        }

        protected override async Task<SetStreamMetadataResult> SetStreamMetadataInternal(
            string streamId,
            int expectedVersion,
            int? maxAge,
            int? maxCount,
            string metadataJson,
            CancellationToken cancellationToken)
        {
            GuardAgainstDisposed();
            cancellationToken.ThrowIfCancellationRequested();

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
                expectedVersion,
                json
            );
            var message = new NewStreamMessage(messageId, "$stream-metadata", json);

            var idinfo = new StreamIdInfo(streamId);
            var result = await AppendToStreamInternal(
                idinfo.MetadataSqlStreamId.IdOriginal,
                expectedVersion,
                new[] { message },
                cancellationToken).ConfigureAwait(false);


            CheckStreamMaxCount(streamId, maxCount, cancellationToken).Wait(cancellationToken);

            return new SetStreamMetadataResult(result.CurrentVersion);
        }
    }
}
