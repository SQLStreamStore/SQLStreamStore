namespace SqlStreamStore
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Threading;
    using System.Threading.Tasks;
    using Microsoft.Data.Sqlite;
    using SqlStreamStore.Infrastructure;
    using SqlStreamStore.Streams;

    public partial class SqliteStreamStore
    {
        protected override Task DeleteStreamInternal(string streamId, int expectedVersion, CancellationToken cancellationToken)
        {
            GuardAgainstDisposed();
            cancellationToken.ThrowIfCancellationRequested();
            
            var info = new StreamIdInfo(streamId);
            return DeleteStreamInternal(info.SqlStreamId, expectedVersion, cancellationToken);
        }

        protected override async Task DeleteEventInternal(string streamId, Guid eventId, CancellationToken cancellationToken)
        {
            GuardAgainstDisposed();
            cancellationToken.ThrowIfCancellationRequested();

            var hasBeenDeleted = false;
            var idInfo = new StreamIdInfo(streamId);
            
            using(var conn = OpenConnection(false))
            using(var txn = conn.BeginTransaction())
            using(var cmd = conn.CreateCommand())
            {
                cmd.Transaction = txn;
                
                cmd.CommandText = @"SELECT COUNT(*) FROM streams WHERE id = @streamId;

SELECT COUNT(*)
FROM messages
    JOIN streams ON messages.stream_id_internal = streams.id_internal
WHERE streams.id = @streamId
    AND messages.event_id = @messageId;

DELETE FROM messages 
    JOIN streams ON messages.stream_id_internal = streams.id_internal
WHERE streams.id = @streamId
    AND messages.event_id = @messageId;
";
                cmd.Parameters.AddWithValue("@streamId", idInfo.SqlStreamId.Id);
                cmd.Parameters.AddWithValue("@messageId", eventId);
                using(var reader = cmd.ExecuteReader())
                {
                    reader.Read();
                    var streamsCount = !reader.IsDBNull(0) 
                        ? reader.GetInt64(0)
                        : -1;
                    if(streamsCount <= 0)
                    {
                        return;
                    }

                    reader.NextResult();
                    reader.Read();
                    hasBeenDeleted = !reader.IsDBNull(0) && (reader.GetInt64(0) > 0);
                }
                
                txn.Commit();
            }

            if(hasBeenDeleted)
            {
                var deletedEvent = Deleted.CreateMessageDeletedMessage(streamId, eventId);
                await AppendToStreamInternal(Deleted.DeletedStreamId, ExpectedVersion.Any, new[] { deletedEvent }, cancellationToken).NotOnCapturedContext();
            }
        }

        private async Task DeleteStreamInternal(SqliteStreamId streamId, int expectedVersion, CancellationToken ct)
        {
            var messages = new List<NewStreamMessage>();
            var idInternal = default(int?);

            try
            {
                // find stream.
                idInternal = ResolveInternalStreamId(streamId.IdOriginal);
            }
            catch(Exception exc)
            {
                throw new WrongExpectedVersionException(
                    ErrorMessages.DeleteStreamFailedWrongExpectedVersion(streamId.IdOriginal, expectedVersion),
                    streamId.IdOriginal,
                    expectedVersion,
                    exc
                );
            }

            var currentStreamVersion = default(long?);
            using(var conn = OpenConnection())
            using(var cmd = conn.CreateCommand())
            {
                using(var txn = conn.BeginTransaction())
                {
                    cmd.Transaction = txn;
                    
                    // delete stream records.
                    messages.Add(DeleteAStream(cmd, streamId, idInternal.Value, ExpectedVersion.Any, out currentStreamVersion, ct));

                    // delete stream metadata records.
                    messages.Add(DeleteAStream(cmd, streamId, idInternal.Value, ExpectedVersion.Any, out currentStreamVersion, ct));

                    txn.Commit();
                }
            }
            
            // delete stream entry.
            await AppendToStreamAnyVersion(Deleted.DeletedStreamId, messages.Where(m=>m != null).ToArray(), ct).NotOnCapturedContext();
        }

        private NewStreamMessage DeleteAStream(SqliteCommand cmd, SqliteStreamId sqliteStreamId, int idInternal, int expectedVersion, out long? currentVersion, CancellationToken ct)
        {
            currentVersion = default(long?);
            
            if(expectedVersion != ExpectedVersion.Any)
            {
                cmd.CommandText = @"SELECT messages.stream_version 
                                        FROM messages 
                                        WHERE messages.stream_id_internal = @streamIdInternal
                                        ORDER BY messages.stream_version DESC 
                                        LIMIT 1;";
                cmd.Parameters.Clear();
                cmd.Parameters.AddWithValue("@streamIdInternal", idInternal);
                currentVersion = cmd.ExecuteScalar<long?>();
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
            cmd.CommandText = @"DELETE FROM messages WHERE messages.stream_id_internal = @idInternal;
                                DELETE FROM streams WHERE streams.id_internal = @idInternal;";


            return Deleted.CreateStreamDeletedMessage(sqliteStreamId.IdOriginal);
        }
    }
}