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
        protected override async Task DeleteStreamInternal(string streamId, int expectedVersion, CancellationToken cancellationToken)
        {
            GuardAgainstDisposed();
            cancellationToken.ThrowIfCancellationRequested();
            
            var info = new StreamIdInfo(streamId);
            var messages = new List<NewStreamMessage>
            {
                DeleteStreamInternal(info.SqlStreamId, expectedVersion, cancellationToken),
                DeleteStreamInternal(info.MetadataSqlStreamId, ExpectedVersion.Any, cancellationToken)
            };

            foreach (var msg in messages.Where(m => m != null))
            {
                await AppendToStreamInternal(Deleted.DeletedStreamId, ExpectedVersion.Any, new [] { msg }, cancellationToken).NotOnCapturedContext();
            }

            await TryScavengeAsync(info.SqlStreamId, cancellationToken);
            await TryScavengeAsync(info.MetadataSqlStreamId, cancellationToken);
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

        private NewStreamMessage DeleteStreamInternal(SqliteStreamId streamId, int expectedVersion, CancellationToken ct)
        {
            using(var connection = OpenConnection())
            using(var transaction = connection.BeginTransaction())
            using(var command = connection.CreateCommand())
            {
                command.Transaction = transaction;
                // determine if the stream exists.
                // if a stream is not found and an expected version is requested, throw a
                // <see cref="WrongExpectedVersionException" />
                command.CommandText = "SELECT id_internal FROM streams WHERE id_original = @streamId";
                command.Parameters.Clear();
                command.Parameters.AddWithValue("@streamId", streamId.IdOriginal);
                var idInternal = command.ExecuteScalar<long?>();
                if(idInternal == null)
                {
                    if(expectedVersion >= 0)
                    {
                        throw new WrongExpectedVersionException(
                            ErrorMessages.DeleteStreamFailedWrongExpectedVersion(streamId.IdOriginal, expectedVersion),
                            streamId.IdOriginal,
                            expectedVersion,
                            default);
                    }

                    return default;
                }

                if(expectedVersion != ExpectedVersion.Any)
                {
                    command.CommandText = @"SELECT messages.stream_version 
                                        FROM messages 
                                        WHERE messages.stream_id_internal = @streamIdInternal
                                        ORDER BY messages.stream_version DESC 
                                        LIMIT 1;";
                    command.Parameters.Clear();
                    command.Parameters.AddWithValue("@streamIdInternal", idInternal);

                    if(expectedVersion != command.ExecuteScalar<long?>())
                    {
                        throw new WrongExpectedVersionException(
                            ErrorMessages.DeleteStreamFailedWrongExpectedVersion(streamId.IdOriginal, expectedVersion),
                            streamId.IdOriginal,
                            expectedVersion);
                    }
                }
                
                command.CommandText = @"DELETE FROM messages WHERE messages.stream_id_internal = @streamIdInternal;";
                command.Parameters.Clear();
                command.Parameters.AddWithValue("@streamIdInternal", idInternal);
                command.ExecuteNonQuery();
                    
                command.CommandText = @"DELETE FROM streams WHERE streams.id_internal = @streamIdInternal;";
                command.Parameters.Clear();
                command.Parameters.AddWithValue("@streamIdInternal", idInternal);
                command.ExecuteNonQuery();

                transaction.Commit();

                return Deleted.CreateStreamDeletedMessage(streamId.IdOriginal);

                //TODO: develop deletion tracking, if required.
            }
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