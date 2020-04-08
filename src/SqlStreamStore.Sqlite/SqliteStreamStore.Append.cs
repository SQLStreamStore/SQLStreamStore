namespace SqlStreamStore
{
    using System;
    using System.Threading;
    using System.Threading.Tasks;
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
            
            AppendResult result;
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

            await CheckStreamMaxCount(streamId, cancellationToken).NotOnCapturedContext();

            return result;
        }

        private Task<AppendResult> AppendToStreamAnyVersion(string streamId, NewStreamMessage[] messages, CancellationToken cancellationToken)
        {
            var internalId = ResolveInternalStreamId(streamId, false) ?? CreateStream(streamId);

            using(var conn = OpenConnection(false))
            using(var txn = conn.BeginTransaction())
            using(var cmd = conn.CreateCommand())
            {
                cmd.Transaction = txn;
                
                // resolve stream version, choosing 0 if not exists.
                cmd.CommandText = @"SELECT MAX(stream_version)
                                    FROM messages
                                    WHERE stream_id_internal = @idInternal";
                cmd.Parameters.Clear();
                cmd.Parameters.AddWithValue("@idInternal", internalId);
                var currentStreamVersion = Convert.ToInt32(cmd.ExecuteScalar<long?>() ?? StreamVersion.Start);
                var currentPosition = Position.Start;
                
                cmd.CommandText = @"INSERT INTO messages(event_id, stream_id_internal, stream_version, created_utc, [type], json_data, json_metadata)
                                    VALUES(@eventId, @idInternal, @streamVersion, @createdUtc, @type, @jsonData, @jsonMetadata);
                                    
                                    SELECT last_insert_rowid();";

                foreach(var msg in messages)
                {
                    cmd.Parameters.Clear();
                    cmd.Parameters.AddWithValue("@idInternal", internalId);

                    cmd.Parameters.AddWithValue("@eventId", messages[0].MessageId);
                    cmd.Parameters.AddWithValue("@type", messages[0].Type);
                    cmd.Parameters.AddWithValue("@jsonData", messages[0].JsonData);
                    cmd.Parameters.AddWithValue("@jsonMetadata", messages[0].JsonMetadata);
                    
                    // incrementing current version (see above, where it is either set to "StreamVersion.Start", or the value in the db.
                    currentStreamVersion += 1;
                    cmd.Parameters.AddWithValue("@streamVersion", currentStreamVersion); // to-resolve.
                    cmd.Parameters.AddWithValue("@createdUtc", GetUtcNow());

                    currentPosition = cmd.ExecuteScalar(StreamVersion.End);
                }
                
                txn.Commit();

                return Task.FromResult(new AppendResult(currentStreamVersion, currentPosition));
            }
        }

        private Task<AppendResult> AppendToStreamEmpty(string streamId, NewStreamMessage[] messages, CancellationToken cancellationToken)
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
                    return Task.FromResult(new AppendResult(ExpectedVersion.NoStream, ExpectedVersion.NoStream));
                }


                var currentStreamVersion = StreamVersion.Start;
                var currentPosition = Position.Start;
                
                cmd.CommandText = @"INSERT INTO messages(event_id, stream_id_internal, stream_version, created_utc, [type], json_data, json_metadata)
                                    VALUES(@eventId, @idInternal, @streamVersion, @createdUtc, @type, @jsonData, @jsonMetadata);
                                    
                                    SELECT last_insert_rowid();";

                foreach(var msg in messages)
                {
                    cmd.Parameters.Clear();
                    cmd.Parameters.AddWithValue("@idInternal", internalId);

                    cmd.Parameters.AddWithValue("@eventId", messages[0].MessageId);
                    cmd.Parameters.AddWithValue("@type", messages[0].Type);
                    cmd.Parameters.AddWithValue("@jsonData", messages[0].JsonData);
                    cmd.Parameters.AddWithValue("@jsonMetadata", messages[0].JsonMetadata);
                    
                    // incrementing current version (see above, where it is either set to "StreamVersion.Start", or the value in the db.
                    currentStreamVersion += 1;
                    cmd.Parameters.AddWithValue("@streamVersion", currentStreamVersion); // to-resolve.
                    cmd.Parameters.AddWithValue("@createdUtc", GetUtcNow());

                    currentPosition = cmd.ExecuteScalar(StreamVersion.End);
                }
                
                txn.Commit();

                return Task.FromResult(new AppendResult(currentStreamVersion, currentPosition));
            }
        }

        private Task<AppendResult> AppendToNonexistentStream(string streamId, NewStreamMessage[] messages, CancellationToken cancellationToken)
        {
            var internalId = ResolveInternalStreamId(streamId, false);
            if(internalId != null) { throw new Exception("Stream already exists."); }
            internalId = CreateStream(streamId);
            return AppendToStreamEmpty(streamId, messages, cancellationToken);
        }

        private Task<AppendResult> AppendToStreamExpectedVersion(string streamId, int expectedVersion, NewStreamMessage[] messages, CancellationToken cancellationToken)
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
                cmd.CommandText = @"SELECT COUNT(*)
                                    FROM messages
                                    WHERE stream_id_internal = @internalId
                                        AND stream_version = @version;";
                cmd.Parameters.Clear();
                cmd.Parameters.AddWithValue("@internalId", internalId);
                cmd.Parameters.AddWithValue("@version", expectedVersion);
                if(cmd.ExecuteScalar<long?>(0) != 0)
                {
                    throw new WrongExpectedVersionException(
                        ErrorMessages.AppendFailedWrongExpectedVersion(
                            streamId,
                            expectedVersion),
                        streamId,
                        expectedVersion);
                }

                var currentStreamVersion = expectedVersion;
                var currentPosition = Position.Start;
                
                cmd.CommandText = @"INSERT INTO messages(event_id, stream_id_internal, stream_version, created_utc, [type], json_data, json_metadata)
                                    VALUES(@eventId, @idInternal, @streamVersion, @createdUtc, @type, @jsonData, @jsonMetadata);
                                    
                                    SELECT last_insert_id();";

                foreach(var msg in messages)
                {
                    cmd.Parameters.Clear();
                    cmd.Parameters.AddWithValue("@idInternal", internalId);

                    cmd.Parameters.AddWithValue("@eventId", messages[0].MessageId);
                    cmd.Parameters.AddWithValue("@type", messages[0].Type);
                    cmd.Parameters.AddWithValue("@jsonData", messages[0].JsonData);
                    cmd.Parameters.AddWithValue("@jsonMetadata", messages[0].JsonMetadata);
                    
                    // incrementing current version (see above, where it is either set to "StreamVersion.Start", or the value in the db.
                    currentStreamVersion += 1;
                    cmd.Parameters.AddWithValue("@streamVersion", currentStreamVersion); // to-resolve.
                    cmd.Parameters.AddWithValue("@createdUtc", GetUtcNow());

                    currentPosition = cmd.ExecuteScalar(StreamVersion.End);
                }
                
                txn.Commit();

                return Task.FromResult(new AppendResult(currentStreamVersion, currentPosition));
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