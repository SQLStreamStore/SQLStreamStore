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
        protected override Task DeleteStreamInternal(
            string streamId, 
            int expectedVersion, 
            CancellationToken cancellationToken)
        {
            var streamIdInfo = new StreamIdInfo(streamId);

            return expectedVersion == ExpectedVersion.Any
                ? DeleteStreamAnyVersion(streamIdInfo, cancellationToken)
                : DeleteStreamExpectedVersion(streamIdInfo, expectedVersion, cancellationToken);
        }

        protected override async Task DeleteEventInternal(
            string streamId, 
            Guid eventId, 
            CancellationToken cancellationToken)
        {
            using (var connection = _createConnection())
            {
                await connection.OpenAsync(cancellationToken);
                using (var transaction = connection.BeginTransaction())
                {
                    var sqlStreamId = new StreamIdInfo(streamId).SQLiteStreamId;
                    bool deleted;
                    using (var command = new SQLiteCommand(_scripts.DeleteEvent, connection, transaction))
                    {
                        command.Parameters.Add(new SQLiteParameter("@streamId", sqlStreamId.Id));
                        command.Parameters.Add(new SQLiteParameter("@eventId", eventId));
                        var count = await command
                            .ExecuteScalarAsync(cancellationToken)
                            .NotOnCapturedContext();

                        deleted = (int) count == 1;
                    }

                    if (deleted)
                    {
                        var eventDeletedEvent = CreateMessageDeletedMessage(sqlStreamId.IdOriginal, eventId);
                        await connection.AppendToStreamExpectedVersionAny(
                            transaction,
                            SQLiteStreamId.Deleted,
                            new[] { eventDeletedEvent },
                            GetUtcNow,
                            cancellationToken);
                    }

                    transaction.Commit();
                }
            }
        }

        private async Task DeleteStreamExpectedVersion(
            StreamIdInfo streamIdInfo,
            int expectedVersion,
            CancellationToken cancellationToken)
        {
            using (var connection = _createConnection())
            {
                await connection.OpenAsync(cancellationToken).NotOnCapturedContext();

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
                        await connection.AppendToStreamExpectedVersionAny(
                            transaction,
                            SQLiteStreamId.Deleted,
                            new [] { streamDeletedEvent },
                            GetUtcNow,
                            cancellationToken);

                        await DeleteStreamAnyVersion(connection,transaction,streamIdInfo.MetadataSQLiteStreamId, cancellationToken);

                        transaction.Commit();
                    }
                }
            }
        }

        private async Task DeleteStreamAnyVersion(
            StreamIdInfo streamIdInfo,
            CancellationToken cancellationToken)
        {
            using (var connection = _createConnection())
            {
                await connection.OpenAsync(cancellationToken);

                using (var transaction = connection.BeginTransaction())
                {
                    await DeleteStreamAnyVersion(connection, transaction, streamIdInfo.SQLiteStreamId, cancellationToken);
                    await DeleteStreamAnyVersion(connection, transaction, streamIdInfo.MetadataSQLiteStreamId, cancellationToken);
                    transaction.Commit();
                }
            }
        }

        private async Task DeleteStreamAnyVersion(
            SQLiteConnection connection, 
            SQLiteTransaction transaction, 
            SQLiteStreamId sQLiteStreamId, 
            CancellationToken cancellationToken)
        {
            bool aStreamIsDeleted;
            var sql = @"DELETE FROM messages
WHERE messages.stream_id_internal = (
      SELECT TOP 1 streams.id_internal 
      FROM streams
      WHERE streams.id = @streamId);

DELETE FROM streams
      WHERE streams.id = @streamId;
SELECT @@ROWCOUNT;";

            using (var command = new SQLiteCommand(sql, connection, transaction))
            {
                command.Parameters.Add(new SQLiteParameter("@streamId", sQLiteStreamId.Id));
                var i = await command
                    .ExecuteScalarAsync(cancellationToken)
                    .NotOnCapturedContext();

                aStreamIsDeleted = (int)i > 0;
            }

            if (aStreamIsDeleted)
            {
                var streamDeletedEvent = CreateStreamDeletedMessage(sQLiteStreamId.IdOriginal);
                await connection.AppendToStreamExpectedVersionAny(
                    transaction, 
                    SQLiteStreamId.Deleted,
                    new[] { streamDeletedEvent },
                    GetUtcNow,
                    cancellationToken);
            }
        }
    }
}