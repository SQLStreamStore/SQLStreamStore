namespace Cedar.EventStore
{
    using System;
    using System.Threading;
    using System.Threading.Tasks;
    using Cedar.EventStore.Streams;

    public partial class MsSqlEventStore
    {
        protected override Task<StreamMetadataResult> GetStreamMetadataInternal(
            string streamId,
            CancellationToken cancellationToken)
        {
            throw new NotImplementedException();
        }

        public override Task SetStreamMetadataInternal(
            string streamId,
            int expectedStreamMetadataVersion,
            int? maxAge,
            int? maxCount,
            string metadataJson,
            CancellationToken cancellationToken)
        {
            throw new NotImplementedException();
        }
    }
}
