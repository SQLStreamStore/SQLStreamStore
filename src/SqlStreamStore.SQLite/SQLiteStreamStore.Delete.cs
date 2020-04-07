namespace SqlStreamStore
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
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
            var messages = new List<NewStreamMessage>();
            
            using(var connection = OpenConnection())
            using(var transaction = connection.BeginTransaction())
            using(var command = connection.CreateCommand())
            {
                command.Transaction = transaction;
                
                messages.Add(DeleteStreamInternal(
                    command,
                    streamIdInfo.SQLiteStreamId,
                    expectedVersion,
                    cancellationToken));

                messages.Add(DeleteStreamInternal(
                    command,
                    streamIdInfo.MetadataSQLiteStreamId,
                    ExpectedVersion.Any,
                    cancellationToken));

                transaction.Commit();
            }
            
            foreach (var msg in messages.Where(m => m != null))
            {
                AppendToStreamInternal(Deleted.DeletedStreamId, ExpectedVersion.Any, new [] { msg }, cancellationToken)
                    .Wait(cancellationToken);
            }

            return Task.CompletedTask;
        }


        private NewStreamMessage DeleteStreamInternal(
            SqliteCommand command,
            SQLiteStreamId streamId,
            int expectedVersion,
            CancellationToken cancellationToken)
        {
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

            
            command.CommandText = @"SELECT messages.stream_version 
                                    FROM messages 
                                    WHERE messages.stream_id_internal = @streamIdInternal
                                    ORDER BY messages.stream_version DESC 
                                    LIMIT 1;";
            command.Parameters.Clear();
            command.Parameters.AddWithValue("@streamIdInternal", idInternal);
            var currentStreamVersion = command.ExecuteScalar<long?>();
            if(expectedVersion != ExpectedVersion.Any && currentStreamVersion != expectedVersion)
            {
                throw new WrongExpectedVersionException(
                    ErrorMessages.DeleteStreamFailedWrongExpectedVersion(streamId.IdOriginal, expectedVersion),
                    streamId.IdOriginal,
                    expectedVersion,
                    default);
            }
            
            command.CommandText = @"DELETE FROM messages WHERE messages.stream_id_internal = @streamIdInternal;";
            command.Parameters.Clear();
            command.Parameters.AddWithValue("@streamIdInternal", idInternal);
            command.ExecuteNonQuery();
                
            command.CommandText = @"DELETE FROM streams WHERE streams.id_internal = @streamIdInternal;";
            command.Parameters.Clear();
            command.Parameters.AddWithValue("@streamIdInternal", idInternal);
            command.ExecuteNonQuery();

            return Deleted.CreateStreamDeletedMessage(streamId.IdOriginal);

            //TODO: develop deletion tracking, if required.
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
                
                transaction.Commit();
            }

            if(hasBeenDeleted)
            {
                var deletedEvent = Deleted.CreateMessageDeletedMessage(streamId, eventId);
                await AppendToStreamInternal(Deleted.DeletedStreamId, ExpectedVersion.Any, new[] { deletedEvent }, cancellationToken);
            }
        }
    }
}