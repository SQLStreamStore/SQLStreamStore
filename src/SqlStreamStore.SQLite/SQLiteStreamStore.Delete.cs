namespace SqlStreamStore
{
    using System;
    using System.Threading;
    using System.Threading.Tasks;
    using Microsoft.Data.Sqlite;
    using SqlStreamStore.Streams;

    public partial class SQLiteStreamStore
    {
        protected override Task DeleteStreamInternal(
            string streamId,
            int expectedVersion,
            CancellationToken cancellationToken)
        {
            var streamIdInfo = new StreamIdInfo(streamId);
            
            using (var connection = OpenConnection())
            using(var transaction = connection.BeginTransaction())
            {
                DeleteStreamInternal(streamIdInfo.SQLiteStreamId,
                    expectedVersion,
                    transaction,
                    cancellationToken);

                DeleteStreamInternal(streamIdInfo.MetadataSQLiteStreamId,
                    expectedVersion,
                    transaction,
                    cancellationToken);

                transaction.Commit();
            }
            
            return Task.CompletedTask;
        }


        private void DeleteStreamInternal(
            SQLiteStreamId streamId,
            int expectedVersion,
            SqliteTransaction transaction,
            CancellationToken cancellationToken)
        {
            int streamIdInternal = 0;
            using(var command = transaction.Connection.CreateCommand())
            {
                command.CommandText = "SELECT id_internal FROM streams WHERE id_original = @streamId";
                command.Parameters.Clear();
                command.Parameters.AddWithValue("@streamId", streamId.IdOriginal);
                var result = command.ExecuteScalar();
                if(result == DBNull.Value || result == null)
                {
                    throw new WrongExpectedVersionException(
                        ErrorMessages.DeleteStreamFailedWrongExpectedVersion(streamId.IdOriginal, expectedVersion),
                        streamId.IdOriginal,
                        expectedVersion,
                        default);
                }

                if(expectedVersion == ExpectedVersion.EmptyStream)
                {
                    throw new WrongExpectedVersionException(
                        ErrorMessages.DeleteStreamFailedWrongExpectedVersion(streamId.IdOriginal, expectedVersion),
                        streamId.IdOriginal,
                        expectedVersion,
                        default);
                }

                streamIdInternal = Convert.ToInt32(result);
                
                if(expectedVersion >= 0) // expected version
                {
                    command.CommandText = @"SELECT messages.stream_version 
                                        FROM messages 
                                        WHERE messages.stream_id_internal = @streamIdInternal 
                                        ORDER BY messages.position DESC
                                        LIMIT 1;";
                    command.Parameters.Clear();
                    command.Parameters.AddWithValue("@streamIdInternal", streamIdInternal);
                    var latestVersion = Convert.ToInt32(command.ExecuteScalar());

                    if(expectedVersion != latestVersion)
                    {
                        throw new WrongExpectedVersionException(
                            ErrorMessages.DeleteStreamFailedWrongExpectedVersion(streamId.IdOriginal, expectedVersion),
                            streamId.IdOriginal,
                            expectedVersion,
                            default);
                    }
                }

                command.CommandText = @"DELETE FROM messages WHERE messages.stream_id_internal = @streamIdInternal;";
                command.Parameters.Clear();
                command.Parameters.AddWithValue("@streamIdInternal", streamIdInternal);
                command.ExecuteNonQuery();
                    
                command.CommandText = @"DELETE FROM streams WHERE streams.id = @streamId;";
                command.Parameters.Clear();
                command.Parameters.AddWithValue("@streamId", streamId.Id);
                command.ExecuteNonQuery();

                //TODO: develop deletion tracking, if required.
            }
        }

        protected override async Task DeleteEventInternal(
            string streamId,
            Guid eventId,
            CancellationToken cancellationToken)
        {
            GuardAgainstDisposed();
            cancellationToken.ThrowIfCancellationRequested();
            
            var streamIdInfo = new StreamIdInfo(streamId);
            bool hasBeenDeleted = false;
            
            using(var connection = OpenConnection())
            using(var transaction = connection.BeginTransaction())
            using (var command = connection.CreateCommand())
            {
                command.Transaction = transaction;
                command.CommandText = @"
SELECT COUNT(*) FROM streams WHERE id = @streamId;

SELECT COUNT(*)
FROM messages
    JOIN streams ON messages.stream_id_internal = streams.id_internal
WHERE streams.id = @streamId
    AND messages.message_id = @messageId;

DELETE FROM messages 
    JOIN streams ON messages.stream_id_internal = streams.id_internal
WHERE streams.id = @streamId
    AND messages.message_id = @messageId;
";
                command.Parameters.AddWithValue("@streamId", streamIdInfo.SQLiteStreamId.Id);
                command.Parameters.AddWithValue("@messageId", eventId);
                using(var reader = command.ExecuteReader())
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
            }

            if(hasBeenDeleted)
            {
                var deletedEvent = Deleted.CreateMessageDeletedMessage(streamId, eventId);
                await AppendToStreamInternal(Deleted.DeletedStreamId, ExpectedVersion.Any, new[] { deletedEvent }, cancellationToken);
            }
        }
    }
}