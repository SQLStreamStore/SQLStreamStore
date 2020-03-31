namespace SqlStreamStore
{
    using System;
    using System.Threading;
    using System.Threading.Tasks;
    using SqlStreamStore.Streams;

    public partial class SQLiteStreamStore
    {
        protected override Task<StreamMetadataResult> GetStreamMetadataInternal(string streamId, CancellationToken cancellationToken)
        {
            throw new NotImplementedException();
        }

        protected override Task<SetStreamMetadataResult> SetStreamMetadataInternal(string streamId, int expectedStreamMetadataVersion, int? maxAge, int? maxCount, string metadataJson, CancellationToken cancellationToken)
        {
            throw new NotImplementedException();
        }

   }
}