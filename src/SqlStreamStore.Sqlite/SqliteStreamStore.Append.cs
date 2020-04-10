namespace SqlStreamStore
{
    using System;
    using System.Threading;
    using System.Threading.Tasks;
    using Microsoft.Data.Sqlite;
    using SqlStreamStore.Infrastructure;
    using SqlStreamStore.Streams;

    public partial class SqliteStreamStore
    {
        protected override async Task<AppendResult> AppendToStreamInternal(
            string streamId,
            int expectedVersion,
            NewStreamMessage[] messages,
            CancellationToken cancellationToken)
        {
            GuardAgainstDisposed();
            cancellationToken.ThrowIfCancellationRequested();
            
            SqliteAppendResult result;
            switch(expectedVersion)
            {
                case ExpectedVersion.Any:
                    result = await AppendToStreamAnyVersion(streamId, messages, cancellationToken).NotOnCapturedContext();
                    break;
                case ExpectedVersion.EmptyStream:
                    result = await AppendToStreamEmpty(streamId, messages, cancellationToken).NotOnCapturedContext();
                    break;
                case ExpectedVersion.NoStream:
                    result = await AppendToNonexistentStream(streamId, messages, cancellationToken).NotOnCapturedContext();
                    break;
                default:
                    result = await AppendToStreamExpectedVersion(streamId, expectedVersion, messages, cancellationToken).NotOnCapturedContext();
                    break;
            }

            UpdateStreamsWithAppendResult(result);

            await CheckStreamMaxCount(streamId, cancellationToken).NotOnCapturedContext();

            return result;
        }

        private void UpdateStreamsWithAppendResult(SqliteAppendResult result)
        {
            using(var conn = OpenConnection(false))
            using(var txn = conn.BeginTransaction())
            using(var cmd = conn.CreateCommand())
            {
                cmd.CommandText = @"UPDATE streams
                                    SET [version] = @version,
                                        [position] = @position
                                    WHERE id_internal = @streamId";
                cmd.Parameters.Clear();
                cmd.Parameters.AddWithValue("@version", result.CurrentVersion);
                cmd.Parameters.AddWithValue("@position", result.CurrentPosition);
                cmd.Parameters.AddWithValue("@streamId", result.InternalStreamId);
                cmd.ExecuteNonQuery();
                
                txn.Commit();
            }
        }

        private Task<SqliteAppendResult> AppendToStreamAnyVersion(string streamId, NewStreamMessage[] messages, CancellationToken cancellationToken)
        {
            var internalId = ResolveInternalStreamId(streamId, false) ?? CreateStream(streamId);

            using(var conn = OpenConnection(false))
            using(var txn = conn.BeginTransaction())
            using(var cmd = conn.CreateCommand())
            {
                cmd.Transaction = txn;
                
                
                // unit tests state that if there is a single message, and it exists already, then
                // do not add the second message into the system.  Send the SqliteAppendResult back
                // as if you had done the insert.
                if(messages.Length == 1)
                {
                    var msg = messages[0];
                    
                    // if the message's event id exists in the database...
                    cmd.CommandText = @"SELECT stream_version, [position] 
                                        FROM messages
                                        WHERE event_id = @eventId AND stream_id_internal = @idInternal;";
                    cmd.Parameters.Clear();
                    cmd.Parameters.AddWithValue("@idInternal", internalId);
                    cmd.Parameters.AddWithValue("@eventId", msg.MessageId);

                    using(var reader = cmd.ExecuteReader())
                    {
                        if(reader.Read())
                        {
                            var ver = reader.ReadScalar<int>(0);
                            var pos = reader.ReadScalar<long>(1);

                            return Task.FromResult(new SqliteAppendResult(ver, pos, internalId));
                        }
                    }
                }
                
                var stored = StoreMessages(messages, cmd, internalId);
                
                txn.Commit();

                return Task.FromResult(new SqliteAppendResult(stored.Version, stored.Position, internalId));
            }
        }

        private Task<SqliteAppendResult> AppendToStreamEmpty(string streamId, NewStreamMessage[] messages, CancellationToken cancellationToken)
        {
            var internalId = ResolveInternalStreamId(streamId);
            
            using(var conn = OpenConnection(false))
            using(var txn = conn.BeginTransaction())
            using(var cmd = conn.CreateCommand())
            {
                cmd.Transaction = txn;
                
                // check to see if the stream has records.  if so, throw wrongexpectedversion exception.
                cmd.CommandText = @"SELECT COUNT(*)
                                    FROM messages
                                    WHERE stream_id_internal = @internalId";
                cmd.Parameters.Clear();
                cmd.Parameters.AddWithValue("@internalId", internalId);
                if(cmd.ExecuteScalar<long?>(0) > 0)
                {
                    // we has information in the stream.
                    return Task.FromResult(new SqliteAppendResult(ExpectedVersion.NoStream, ExpectedVersion.NoStream, internalId ?? -1));
                }
                                
                var stored = StoreMessages(messages, cmd, Convert.ToInt32(internalId));
                
                txn.Commit();

                return Task.FromResult(new SqliteAppendResult(stored.Version, stored.Position, Convert.ToInt32(internalId)));
            }
        }

        private Task<SqliteAppendResult> AppendToNonexistentStream(string streamId, NewStreamMessage[] messages, CancellationToken cancellationToken)
        {
            var internalId = ResolveInternalStreamId(streamId, false);
            if(internalId > Position.Start)
            {
                throw new WrongExpectedVersionException(
                    ErrorMessages.AppendFailedWrongExpectedVersion(
                        streamId,
                        ExpectedVersion.NoStream),
                    streamId,
                    ExpectedVersion.NoStream);
            }
            
            CreateStream(streamId);
            
            return AppendToStreamEmpty(streamId, messages, cancellationToken);
        }

        private Task<SqliteAppendResult> AppendToStreamExpectedVersion(string streamId, int expectedVersion, NewStreamMessage[] messages, CancellationToken cancellationToken)
        {
            var internalId = ResolveInternalStreamId(streamId, false);
            if(internalId == null)
            {
                throw new WrongExpectedVersionException(
                    ErrorMessages.AppendFailedWrongExpectedVersion(
                        streamId,
                        expectedVersion),
                    streamId,
                    expectedVersion);
            }
            
            using(var conn = OpenConnection(false))
            using(var txn = conn.BeginTransaction())
            using(var cmd = conn.CreateCommand())
            {
                cmd.Transaction = txn;
                
                // check to see if the stream has records.  if so, throw wrongexpectedversion exception.
                cmd.CommandText = @"SELECT [version]
                                    FROM streams
                                    WHERE id_internal = @id";
                cmd.Parameters.Clear();
                cmd.Parameters.AddWithValue("@id", internalId);
                int actualStreamVersion = cmd.ExecuteScalar<int>();

                if(actualStreamVersion != expectedVersion) // we have to check length because of add'l rules around single-message processing.
                {
                    if(messages.Length == 1)
                    {
                        cmd.CommandText = @"SELECT streams.[version], streams.[position]
                                        FROM streams
                                        WHERE streams.id_internal = @internalId";
                        cmd.Parameters.Clear();
                        cmd.Parameters.AddWithValue("@internalId", internalId);

                        using(var reader = cmd.ExecuteReader())
                        {
                            if(reader.Read())
                            {
                                var pos = reader.ReadScalar(0, Position.Start);
                                var ver = reader.ReadScalar(1, StreamVersion.Start);

                                return Task.FromResult(new SqliteAppendResult(ver, pos, internalId ?? -1));
                            }
                        }
                    }

                    // determine if second post.  if so, return position/version as if
                    // it was the first post.
                    throw new WrongExpectedVersionException(
                        ErrorMessages.AppendFailedWrongExpectedVersion(
                            streamId,
                            expectedVersion),
                        streamId,
                        expectedVersion);
                }

                var result = StoreMessages(messages, cmd, internalId.Value);
                
                txn.Commit();

                return Task.FromResult(new SqliteAppendResult(result.Version, result.Position, internalId ?? -1));
            }
        }

        private int CreateStream(string streamId, bool throwIfCreateFails = true)
        {
            using(var conn = OpenConnection(false))
            using(var cmd = conn.CreateCommand())
            {
                cmd.CommandText = @"SELECT streams.id_internal
                                    FROM streams
                                    WHERE id_original = @streamId;";
                cmd.Parameters.Clear();
                cmd.Parameters.AddWithValue("@streamId", streamId);

                var idInternal = cmd.ExecuteScalar<int?>();
                if(idInternal != null)
                {
                    return idInternal.Value;
                }

                using(var txn = conn.BeginTransaction())
                {
                    cmd.Transaction = txn;
                    
                    var info = new StreamIdInfo(streamId);
                    cmd.CommandText = @"INSERT INTO streams (id, id_original)
                                        VALUES (@id, @idOriginal);
                                        
                                        SELECT last_insert_rowid();";
                    cmd.Parameters.AddWithValue("@id", info.SqlStreamId.Id);
                    cmd.Parameters.AddWithValue("@idOriginal", info.SqlStreamId.IdOriginal);
                    var inserted = cmd.ExecuteScalar<int?>();
                    
                    txn.Commit();

                    if(inserted == null && throwIfCreateFails)
                    {
                        throw new Exception("Stream failed to create.");
                    }

                    return inserted ?? int.MinValue;
                }
            }
        }
 
        private (int Version, long Position) StoreMessages(NewStreamMessage[] messages, SqliteCommand cmd, int internalId)
        {
            int version = 0;
            long position = 0;
            
            // resolve stream version, choosing 0 if not exists.
            cmd.CommandText = @"SELECT MAX(stream_version)
                                    FROM messages
                                    WHERE stream_id_internal = @idInternal;
                                SELECT MAX([position])
                                FROM messages;";
            cmd.Parameters.Clear();
            cmd.Parameters.AddWithValue("@idInternal", internalId);
            using(var reader = cmd.ExecuteReader())
            {
                reader.Read();
                version = reader.ReadScalar(0, StreamVersion.End);

                reader.NextResult();
                reader.Read();
                position = reader.ReadScalar(0, Position.End);
            }

            foreach(var msg in messages)
            {
                cmd.CommandText = @"SELECT COUNT(*)
                                        FROM messages
                                        WHERE event_id = @eventId AND stream_id_internal = @idInternal;";
                cmd.Parameters.Clear();
                cmd.Parameters.AddWithValue("@idInternal", internalId);
                cmd.Parameters.AddWithValue("@eventId", msg.MessageId);
                var hasMessages = (cmd.ExecuteScalar(0) > 0);
                if(hasMessages) continue;

                cmd.CommandText =
                    @"INSERT INTO messages(event_id, stream_id_internal, stream_version, [position], created_utc, [type], json_data, json_metadata)
                                    VALUES(@eventId, @idInternal, @streamVersion, @position, @createdUtc, @type, @jsonData, @jsonMetadata);
                                    
                                    SELECT last_insert_rowid();";

                cmd.Parameters.Clear();

                // incrementing current version (see above, where it is either set to "StreamVersion.Start", or the value in the db.
                version += 1;
                position += 1;

                cmd.Parameters.AddWithValue("@idInternal", internalId);
                cmd.Parameters.AddWithValue("@eventId", msg.MessageId);
                cmd.Parameters.AddWithValue("@type", msg.Type);
                cmd.Parameters.AddWithValue("@jsonData", msg.JsonData);
                cmd.Parameters.AddWithValue("@jsonMetadata", msg.JsonMetadata);
                cmd.Parameters.AddWithValue("@position", position);
                cmd.Parameters.AddWithValue("@streamVersion", version);
                cmd.Parameters.AddWithValue("@createdUtc", GetUtcNow());
                cmd.ExecuteNonQuery();
            }

            return (version, position);
        }
       
        private async Task CheckStreamMaxCount(
            string streamId, 
            CancellationToken cancellationToken)
        {
            //RESEARCH: Would we ever want to perform cleanup function for $deleted?
            if(streamId == Deleted.DeletedStreamId) { return; }
            
            var maxCount = await ResolveStreamMaxCount(streamId).NotOnCapturedContext();
            
            if (maxCount.HasValue)
            {
                var count = await GetStreamMessageCount(streamId, cancellationToken).NotOnCapturedContext();
                if (count > maxCount.Value)
                {
                    int toPurge = count - maxCount.Value;

                    var streamMessagesPage = await ReadStreamForwardsInternal(streamId, StreamVersion.Start, toPurge, false, null, cancellationToken).NotOnCapturedContext();

                    if (streamMessagesPage.Status == PageReadStatus.Success)
                    {
                        foreach (var message in streamMessagesPage.Messages)
                        {
                            await DeleteEventInternal(streamId, message.MessageId, cancellationToken).NotOnCapturedContext();
                        }
                    }
                }
            }
        }

        private Task<int?> ResolveStreamMaxCount(string streamId)
        {
            using(var connection = OpenConnection())
            using(var command = connection.CreateCommand())
            {
                command.CommandText = @"SELECT max_count
                                        FROM streams
                                        WHERE streams.id_original = @idOriginal";
                command.Parameters.AddWithValue("@idOriginal", streamId);

                return Task.FromResult(command.ExecuteScalar<int?>());
            }
        }

        private Task<int> GetStreamMessageCount(
            string streamId,
            CancellationToken cancellationToken = default)
        {
            GuardAgainstDisposed();

            using(var connection = OpenConnection())
            using(var command = connection.CreateCommand())
            {
                command.CommandText = @"SELECT COUNT(*)
                                        FROM messages
                                        JOIN streams on messages.stream_id_internal = streams.id_internal
                                        WHERE streams.id_original = @idOriginal";
                command.Parameters.AddWithValue("@idOriginal", streamId);

                return Task.FromResult(command.ExecuteScalar(0));
            }
        }
    }
}