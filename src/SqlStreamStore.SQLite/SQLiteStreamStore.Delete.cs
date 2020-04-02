namespace SqlStreamStore
{
    using System;
    using System.Data.SQLite;
    using System.Threading;
    using System.Threading.Tasks;
    using SqlStreamStore.Infrastructure;
    using SqlStreamStore.Streams;
    using static Streams.Deleted;

    public partial class SQLiteStreamStore
    {
        protected override async Task DeleteStreamInternal(
            string streamId,
            int expectedVersion,
            CancellationToken cancellationToken)
        {
            var streamIdInfo = new StreamIdInfo(streamId);
            
            using (var connection = await OpenConnection(cancellationToken))
            using(var transaction = connection.BeginTransaction())
            {
                await DeleteStreamInternal(streamIdInfo.SQLiteStreamId,
                    expectedVersion,
                    transaction,
                    cancellationToken);

                await DeleteStreamInternal(streamIdInfo.MetadataSQLiteStreamId,
                    expectedVersion,
                    transaction,
                    cancellationToken);

                transaction.Commit();
            }
        }


        private async Task DeleteStreamInternal(
            SQLiteStreamId streamId,
            int expectedVersion,
            SQLiteTransaction transaction,
            CancellationToken cancellationToken)
        {
            if(expectedVersion == ExpectedVersion.EmptyStream)
            {
                throw new WrongExpectedVersionException(
                    ErrorMessages.DeleteStreamFailedWrongExpectedVersion(streamId.IdOriginal, expectedVersion),
                    streamId.IdOriginal,
                    expectedVersion,
                    default);
            }

            int streamIdInternal = 0;
            using(var command = transaction.Connection.CreateCommand())
            {
                command.CommandText = "SELECT id_internal FROM streams WHERE id = @streamId";
                command.Parameters.Clear();
                command.Parameters.Add(new SQLiteParameter("@streamId", streamId.Id));
                var result = await command.ExecuteScalarAsync(cancellationToken);
                if(result == DBNull.Value)
                {
                    throw new WrongExpectedVersionException(
                        ErrorMessages.DeleteStreamFailedWrongExpectedVersion(streamId.IdOriginal, expectedVersion),
                        streamId.IdOriginal,
                        expectedVersion,
                        default);
                }

                streamIdInternal = Convert.ToInt32(result);

                command.CommandText = @"SELECT messages.stream_version 
                                        FROM messages 
                                        WHERE messages.stream_id_internal = @streamIdInternal 
                                        ORDER BY messages.position DESC
                                        LIMIT 1";
                command.Parameters.Clear();
                command.Parameters.Add(new SQLiteParameter("@streamIdInternal", streamIdInternal));
                var latestVersion = Convert.ToInt32(await command.ExecuteScalarAsync(cancellationToken));

                if(expectedVersion != latestVersion)
                {
                    throw new WrongExpectedVersionException(
                        ErrorMessages.DeleteStreamFailedWrongExpectedVersion(streamId.IdOriginal, expectedVersion),
                        streamId.IdOriginal,
                        expectedVersion,
                        default);
                }

                command.CommandText = @"DELETE FROM messages WHERE messages.stream_id_internal = @streamIdInternal;
                DELETE FROM streams WHERE streams.id = @streamId";
                command.Parameters.Clear();
                command.Parameters.Add(new SQLiteParameter("@streamIdInternal", streamIdInternal));
                command.Parameters.Add(new SQLiteParameter("@streamId", streamId.Id));
                await command.ExecuteNonQueryAsync(cancellationToken);
                
                //TODO: develop deletion tracking, if required.
            }
        }

        protected override async Task DeleteEventInternal(
            string streamId,
            Guid eventId,
            CancellationToken cancellationToken)
        {
            var streamIdInfo = new StreamIdInfo(streamId);
            using(var connection = await OpenConnection(cancellationToken))
            using(var transaction = connection.BeginTransaction())
            {
                await DeleteEventInternal(streamIdInfo, eventId, transaction, cancellationToken);
                
                transaction.Commit();
            }
        }

        private async Task DeleteEventInternal(
            StreamIdInfo streamIdInfo,
            Guid eventId,
            SQLiteTransaction transaction,
            CancellationToken cancellationToken)
        {
            // Note: WE may need to put work in to append to stream expectedversionany.  reference DeleteStreamMessage.sql for more information.
            // var deletedMessageMessage = Deleted.CreateMessageDeletedMessage(
            //     streamIdInfo.SQLiteStreamId.IdOriginal,
            //     eventId);

            using(var command = transaction.Connection.CreateCommand())
            {
                command.CommandText = @"DELETE FROM messages 
                WHERE messages.stream_id_internal = (SELECT id_internal FROM streams WHERE streams.id = @streamId)
                    AND messages.message_id = @messageId";
                command.Parameters.Clear();
                command.Parameters.Add(new SQLiteParameter("@streamId", streamIdInfo.SQLiteStreamId.Id));
                command.Parameters.Add(new SQLiteParameter("@messageId", eventId));

                await command.ExecuteNonQueryAsync(cancellationToken);
            }
        }

        private async Task DeleteStreamExpectedVersion(
            StreamIdInfo streamIdInfo,
            int expectedVersion,
            CancellationToken cancellationToken)
        {
            using (var connection = await OpenConnection(cancellationToken))
            {
                using (var transaction = connection.BeginTransaction())
                {
                    using (var command = new SQLiteCommand("", connection, transaction))
                    {
                        int idInternal = -1;

                        command.CommandText = @"SELECT streams.id_internal
                        FROM streams
                        WHERE streams.id = @streamId";
                        command.Parameters.Add(new SQLiteParameter("@streamId", streamIdInfo.SQLiteStreamId.Id ));
                        var result = await command.ExecuteScalarAsync(cancellationToken).NotOnCapturedContext();
                        if (result == DBNull.Value) 
                        { 
                            throw new WrongExpectedVersionException(
                                ErrorMessages.DeleteStreamFailedWrongExpectedVersion(streamIdInfo.SQLiteStreamId.IdOriginal, expectedVersion),
                                streamIdInfo.SQLiteStreamId.IdOriginal,
                                expectedVersion);
                        }
                        idInternal = (int)result;


                        command.CommandText = @"SELECT TOP(1) messages.stream_version 
                        FROM messages
                        WHERE messages.stream_id_internal = @streamIdInternal
                        ORDER By messages.position DESC;";
                        command.Parameters.Clear();
                        command.Parameters.Add(new SQLiteParameter("@streamIdInternal", idInternal));
                        result = await command.ExecuteScalarAsync(cancellationToken).NotOnCapturedContext();
                        if (result == DBNull.Value || Convert.ToInt32(result) != expectedVersion) 
                        { 
                            throw new WrongExpectedVersionException(
                                ErrorMessages.DeleteStreamFailedWrongExpectedVersion(streamIdInfo.SQLiteStreamId.IdOriginal, expectedVersion),
                                streamIdInfo.SQLiteStreamId.IdOriginal,
                                expectedVersion);
                        }


                        command.CommandText = @"DELETE FROM messages WHERE messages.stream_id_internal = @streamIdInternal;
                        DELETE FROM streams WHERE streams.id = @streamId;";
                        command.Parameters.Clear();
                        command.Parameters.Add(new SQLiteParameter("@streamIdInternal", idInternal));
                        command.Parameters.Add(new SQLiteParameter("@streamId", streamIdInfo.SQLiteStreamId.Id));
                        await command.ExecuteNonQueryAsync(cancellationToken).NotOnCapturedContext();

                        var streamDeletedEvent = CreateStreamDeletedMessage(streamIdInfo.SQLiteStreamId.IdOriginal);
                        await AppendToStreamExpectedVersionAny(command,
                            streamIdInfo.SQLiteStreamId,
                            streamDeletedEvent,
                            cancellationToken);

                        await DeleteStreamAnyVersion(command, streamIdInfo.MetadataSQLiteStreamId, cancellationToken);

                        transaction.Commit();
                    }
                }
            }
        }

        private async Task DeleteStreamAnyVersion(
            StreamIdInfo streamIdInfo,
            CancellationToken cancellationToken)
        {
            using (var connection = await OpenConnection(cancellationToken))
            using (var transaction = connection.BeginTransaction())
            using (var command = connection.CreateCommand())
            {
                command.Transaction = transaction;
                
                await DeleteStreamAnyVersion(command, streamIdInfo.SQLiteStreamId, cancellationToken);
                await DeleteStreamAnyVersion(command, streamIdInfo.MetadataSQLiteStreamId, cancellationToken);
                transaction.Commit();
            }
        }

        private async Task DeleteStreamAnyVersion(
            SQLiteCommand command, 
            SQLiteStreamId streamId, 
            CancellationToken cancellationToken)
        {
            bool aStreamIsDeleted;

            command.CommandText = @"DELETE FROM messages
WHERE messages.stream_id_internal = (
      SELECT TOP 1 streams.id_internal 
      FROM streams
      WHERE streams.id = @streamId);

DELETE FROM streams
      WHERE streams.id = @streamId;
SELECT @@ROWCOUNT;";
            command.Parameters.Clear();
            command.Parameters.Add(new SQLiteParameter("@streamId", streamId.Id));
            var i = await command
                .ExecuteScalarAsync(cancellationToken)
                .NotOnCapturedContext();

            aStreamIsDeleted = (int)i > 0;
            if (aStreamIsDeleted)
            {
                var streamDeletedEvent = CreateStreamDeletedMessage(streamId.Id);
                await AppendToStreamExpectedVersionAny(command,
                streamId,
                streamDeletedEvent,
                cancellationToken);
            }
        }
    }
}