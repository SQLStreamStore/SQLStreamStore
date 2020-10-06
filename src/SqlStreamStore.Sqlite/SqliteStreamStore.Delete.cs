namespace SqlStreamStore
{
    using System;
    using System.Threading;
    using System.Threading.Tasks;
    using SqlStreamStore.Infrastructure;
    using SqlStreamStore.Streams;
    using static Streams.Deleted;

    public partial class SqliteStreamStore
    {
        protected override Task DeleteStreamInternal(string streamId, int expectedVersion, CancellationToken cancellationToken)
        {
            GuardAgainstDisposed();
            cancellationToken.ThrowIfCancellationRequested();


            return expectedVersion == ExpectedVersion.Any
                ? DeleteStreamAnyVersion(streamId, cancellationToken)
                : DeleteStreamExpectedVersion(streamId, expectedVersion, cancellationToken);
        }

        protected override async Task DeleteEventInternal(string streamId, Guid eventId, CancellationToken cancellationToken)
        {
            GuardAgainstDisposed();
            cancellationToken.ThrowIfCancellationRequested();

            bool hasBeenDeleted = false;
            using(var conn = OpenConnection(false))
            {
                var stream = conn.Streams(streamId);
                using(stream.WithTransaction())
                {
                    hasBeenDeleted = await stream.RemoveEvent(eventId, cancellationToken);
                    await stream.Commit(cancellationToken);
                }
            }

            if(hasBeenDeleted)
            {
                var deletedEvent = CreateMessageDeletedMessage(streamId, eventId);
                await AppendToStreamInternal(DeletedStreamId, ExpectedVersion.Any, new[] { deletedEvent }, cancellationToken).ConfigureAwait(false);
            }
        }

        private async Task DeleteStreamAnyVersion(string streamId, CancellationToken cancellationToken)
        {
            (bool StreamDeleted, bool MetadataDeleted) result;
            using(var connection = OpenConnection(false))
            {
                var all = connection.AllStream();
                using(all.WithTransaction())
                {
                    result = await all
                        .Delete(streamId, ExpectedVersion.Any, cancellationToken: cancellationToken);

                    await all.Commit(cancellationToken);
                }
            }

            var streamIdInfo = new StreamIdInfo(streamId);

            if(result.StreamDeleted)
            {
                var streamDeletedEvent = CreateStreamDeletedMessage(streamIdInfo.SqlStreamId.IdOriginal);
                AppendToStreamInternal(DeletedStreamId, ExpectedVersion.Any, new[] { streamDeletedEvent }, cancellationToken).Wait(cancellationToken);
            }

            if(result.MetadataDeleted)
            {
                var streamDeletedEvent = CreateStreamDeletedMessage(streamIdInfo.MetadataSqlStreamId.IdOriginal);
                AppendToStreamInternal(DeletedStreamId, ExpectedVersion.Any, new[] { streamDeletedEvent }, cancellationToken).Wait(cancellationToken);
            }
        }

        private async Task DeleteStreamExpectedVersion(string streamId, int expectedVersion, CancellationToken cancellationToken)
        {
            var id = ResolveInternalStreamId(streamId, throwIfNotExists: false);
            if(id == null)
            {
                throw new WrongExpectedVersionException(
                    ErrorMessages.DeleteStreamFailedWrongExpectedVersion(streamId, expectedVersion),
                    streamId,
                    expectedVersion
                );
            }

            (bool StreamDeleted, bool MetadataDeleted) result;

            using(var connection = OpenConnection(false))
            {
                result = await connection.AllStream()
                    .Delete(streamId, expectedVersion, cancellationToken);
            }

            if (result.StreamDeleted)
            {
                var streamDeletedEvent = CreateStreamDeletedMessage(streamId);
                AppendToStreamInternal(DeletedStreamId, ExpectedVersion.Any, new[] { streamDeletedEvent }, cancellationToken).Wait(cancellationToken);
            }
        }
    }
}
