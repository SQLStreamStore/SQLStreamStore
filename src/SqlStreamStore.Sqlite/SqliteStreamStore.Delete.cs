namespace SqlStreamStore
{
    using System;
    using System.Threading;
    using System.Threading.Tasks;
    using Microsoft.Data.Sqlite;
    using SqlStreamStore.Infrastructure;
    using SqlStreamStore.Streams;
    using static Streams.Deleted;

    public partial class SqliteStreamStore
    {
        protected override Task DeleteStreamInternal(string streamId, int expectedVersion, CancellationToken cancellationToken)
        {
            GuardAgainstDisposed();
            cancellationToken.ThrowIfCancellationRequested();
            
            var streamIdInfo = new StreamIdInfo(streamId);

            return expectedVersion == ExpectedVersion.Any
                ? DeleteStreamAnyVersion(streamIdInfo, cancellationToken)
                : DeleteStreamExpectedVersion(streamIdInfo, expectedVersion, cancellationToken);
        }

        protected override async Task DeleteEventInternal(string streamId, Guid eventId, CancellationToken cancellationToken)
        {
            GuardAgainstDisposed();
            cancellationToken.ThrowIfCancellationRequested();

            var idInfo = new StreamIdInfo(streamId);
            bool streamExists;
            bool willBeDeleted;
            
            using(var conn = OpenConnection(false))
            using(var txn = conn.BeginTransaction())
            using(var cmd = conn.CreateCommand())
            {
                cmd.Transaction = txn;

                cmd.CommandText = @"SELECT COUNT(*) FROM streams WHERE id = @streamId;";
                cmd.Parameters.Clear();
                cmd.Parameters.AddWithValue("@streamId", idInfo.SqlStreamId.Id);
                streamExists= cmd.ExecuteScalar<int>() > 0;

                cmd.CommandText = @"SELECT COUNT(*)
FROM messages
    JOIN streams ON messages.stream_id_internal = streams.id_internal 
WHERE streams.id = @streamId
    AND messages.event_id = @messageId;";
                cmd.Parameters.Clear();
                cmd.Parameters.AddWithValue("@streamId", idInfo.SqlStreamId.Id);
                cmd.Parameters.AddWithValue("@messageId", eventId);
                willBeDeleted = cmd.ExecuteScalar<int>() > 0;

                if(streamExists && willBeDeleted)
                {

                    cmd.CommandText = @"DELETE FROM messages WHERE messages.event_id = @messageId;";
                    cmd.Parameters.Clear();
                    cmd.Parameters.AddWithValue("@messageId", eventId);
                    cmd.ExecuteNonQuery();
                }

                txn.Commit();
            }

            if(streamExists && willBeDeleted)
            {
                var deletedEvent = Deleted.CreateMessageDeletedMessage(streamId, eventId);
                await AppendToStreamInternal(Deleted.DeletedStreamId, ExpectedVersion.Any, new[] { deletedEvent }, cancellationToken).NotOnCapturedContext();
            }
        }

        private Task DeleteStreamAnyVersion(StreamIdInfo sqlStreamId, CancellationToken cancellationToken)
        {
            var streamDeleted = false;
            var metadataDeleted = false;
            using(var connection = OpenConnection(false))
            using(var command = connection.CreateCommand())
            using(var transaction = connection.BeginTransaction())
            {
                command.Transaction = transaction;

                streamDeleted = DeleteAStream(command, sqlStreamId.SqlStreamId, ExpectedVersion.Any, cancellationToken);
                metadataDeleted = DeleteAStream(command, sqlStreamId.MetadataSqlStreamId, ExpectedVersion.Any, cancellationToken);
                
                transaction.Commit();
            }

            if(streamDeleted)
            {
                var streamDeletedEvent = CreateStreamDeletedMessage(sqlStreamId.SqlStreamId.IdOriginal);
                AppendToStreamInternal(DeletedStreamId, ExpectedVersion.Any, new[] { streamDeletedEvent }, cancellationToken).Wait(cancellationToken);
            }

            if(metadataDeleted)
            {
                var streamDeletedEvent = CreateStreamDeletedMessage(sqlStreamId.MetadataSqlStreamId.IdOriginal);
                AppendToStreamInternal(DeletedStreamId, ExpectedVersion.Any, new[] { streamDeletedEvent }, cancellationToken).Wait(cancellationToken);
            }            
            return Task.CompletedTask;
        }

        private Task DeleteStreamExpectedVersion(StreamIdInfo streamIdInfo, int expectedVersion, CancellationToken cancellationToken)
        {
            var id = ResolveInternalStreamId(streamIdInfo.SqlStreamId.IdOriginal, throwIfNotExists: false);
            if(id == null)
            {
                throw new WrongExpectedVersionException(
                    ErrorMessages.DeleteStreamFailedWrongExpectedVersion(streamIdInfo.SqlStreamId.IdOriginal, expectedVersion),
                    streamIdInfo.SqlStreamId.IdOriginal,
                    expectedVersion
                );
            }

            bool hasBeenDeleted = false;
            using(var conn = OpenConnection(false))
            using(var cmd = conn.CreateCommand())
            using(var txn = conn.BeginTransaction())
            {
                cmd.Transaction = txn;
                
                hasBeenDeleted = DeleteAStream(cmd, streamIdInfo.SqlStreamId, expectedVersion, cancellationToken) || hasBeenDeleted;
                hasBeenDeleted = DeleteAStream(cmd, streamIdInfo.MetadataSqlStreamId, ExpectedVersion.Any, cancellationToken) || hasBeenDeleted;

                txn.Commit();
            }

            if (hasBeenDeleted)
            {
                var streamDeletedEvent = CreateStreamDeletedMessage(streamIdInfo.SqlStreamId.IdOriginal);
                AppendToStreamInternal(DeletedStreamId, ExpectedVersion.Any, new[] { streamDeletedEvent }, cancellationToken).Wait(cancellationToken);
            }

            return Task.CompletedTask;
        }

        private bool DeleteAStream(SqliteCommand cmd, SqliteStreamId sqliteStreamId, int expectedVersion, CancellationToken ct)
        {
            if(expectedVersion != ExpectedVersion.Any)
            {
                cmd.CommandText = @"SELECT messages.stream_version 
                                    FROM messages 
                                    WHERE messages.stream_id_internal = (SELECT id_internal FROM streams WHERE streams.id_original = @streamId)
                                    ORDER BY messages.stream_version DESC 
                                    LIMIT 1;";
                cmd.Parameters.Clear();
                cmd.Parameters.AddWithValue("@streamId", sqliteStreamId.IdOriginal);
                var currentVersion = cmd.ExecuteScalar<long?>();

                if(currentVersion != expectedVersion)
                {
                    throw new WrongExpectedVersionException(
                        ErrorMessages.DeleteStreamFailedWrongExpectedVersion(sqliteStreamId.IdOriginal, expectedVersion),
                        sqliteStreamId.IdOriginal,
                        expectedVersion
                    );
                }
            }
            
            // delete stream records.
            cmd.CommandText = @"SELECT COUNT(*) FROM messages WHERE messages.stream_id_internal = (SELECT id_internal FROM streams WHERE streams.id_original = @streamId);
                                SELECT COUNT(*) FROM streams WHERE streams.id_original = @streamId;
                                DELETE FROM messages          WHERE messages.stream_id_internal = (SELECT id_internal FROM streams WHERE streams.id_original = @streamId);
                                DELETE FROM streams WHERE streams.id_original = @streamId;";
            cmd.Parameters.Clear();
            cmd.Parameters.AddWithValue("@streamId", sqliteStreamId.IdOriginal);
            using(var reader = cmd.ExecuteReader())
            {
                reader.Read();
                var numberOfMessages = reader.ReadScalar<int?>(0);

                reader.NextResult();
                reader.Read();
                var numberOfStreams = reader.ReadScalar<int?>(0);

                return (numberOfMessages + numberOfStreams) > 0;
            }
        }
    }
}