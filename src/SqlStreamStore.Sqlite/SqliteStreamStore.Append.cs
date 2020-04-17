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
    using StreamStoreStore.Json;

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
            StreamIdInfo streamIdInfo = new StreamIdInfo(streamId);
            
            switch(expectedVersion)
            {
                case ExpectedVersion.Any:
                    result = await AppendToStreamAnyVersion(streamIdInfo.SqlStreamId, messages, cancellationToken).NotOnCapturedContext();
                    break;
                case ExpectedVersion.EmptyStream:
                    result = await AppendToStreamEmpty(streamIdInfo.SqlStreamId, messages, cancellationToken).NotOnCapturedContext();
                    break;
                case ExpectedVersion.NoStream:
                    result = await AppendToNonexistentStream(streamIdInfo.SqlStreamId, messages, cancellationToken).NotOnCapturedContext();
                    break;
                default:
                    result = AppendToStreamExpectedVersion(streamIdInfo.SqlStreamId, expectedVersion, messages, cancellationToken);
                    break;
            }

            if(result.MaxCount.HasValue)
            {
                await CheckStreamMaxCount(streamId, result.MaxCount, cancellationToken).NotOnCapturedContext();
            }

            await TryScavengeAsync(streamId, cancellationToken);

            return result;
        }

        private Task<SqliteAppendResult> AppendToStreamAnyVersion(SqliteStreamId streamId, NewStreamMessage[] messages, CancellationToken cancellationToken)
        {
            using(var conn = OpenConnection(false))
            using(var txn = conn.BeginTransaction())
            using(var cmd = conn.CreateCommand())
            {
                cmd.Transaction = txn;

                var stored = AppendToStreamAnyVersion(cmd, streamId, messages, cancellationToken);
                
                txn.Commit();
                
                return Task.FromResult(stored);
            }
        }

        private SqliteAppendResult AppendToStreamAnyVersion(
            SqliteCommand cmd,
            SqliteStreamId streamId,
            NewStreamMessage[] messages,
            CancellationToken cancellationToken)
        {
            var internalId = ResolveInternalStreamId(streamId.IdOriginal, cmd, false) ?? CreateStream(cmd, streamId);
            // unit tests state that if there is a single message, and it exists already, then
            // do not add the second message into the system.  Send the SqliteAppendResult back
            // as if you had done the insert.
            if(messages.Length == 1)
            {
                var msg = messages[0];

                // if the message's event id exists in the database...
                cmd.CommandText = @"SELECT count(*) 
                                        FROM messages
                                        WHERE event_id = @eventId AND stream_id_internal = @idInternal;";
                cmd.Parameters.Clear();
                cmd.Parameters.AddWithValue("@idInternal", internalId);
                cmd.Parameters.AddWithValue("@eventId", msg.MessageId);

                var existsInStream = cmd.ExecuteScalar<long>(0) > 0;
                if(existsInStream)
                {
                    cmd.CommandText = @"SELECT [version], [position]
                                        FROM streams
                                        WHERE id_internal = @idInternal;";
                    cmd.Parameters.Clear();
                    cmd.Parameters.AddWithValue("@idInternal", internalId);

                    using(var reader = cmd.ExecuteReader())
                    if(reader.Read())
                    {
                        var ver = reader.ReadScalar<int>(0);
                        var pos = reader.ReadScalar<long>(1);

                        {
                            return new SqliteAppendResult(ver, pos, null);
                        }
                    }
                }
            }
            else if (messages.Any())
            {
                var msg1 = messages.First();
                cmd.CommandText = @"SELECT event_id
                                    FROM messages
                                    WHERE [position] >= (SELECT [position] 
                                                      FROM messages 
                                                      WHERE messages.event_id = @messageId
                                                        AND messages.stream_id_internal = @internalId)
                                        AND stream_id_internal = @internalId;";
                cmd.Parameters.Clear();
                cmd.Parameters.AddWithValue("@messageId", msg1.MessageId);
                cmd.Parameters.AddWithValue("@internalId", internalId);
                cmd.Parameters.AddWithValue("@count", messages.Length);

                var eventIds = new List<Guid>();
                using(var reader = cmd.ExecuteReader())
                {
                    while(reader.Read())
                    {
                        eventIds.Add(reader.ReadScalar<Guid>(0, Guid.Empty));
                    }

                    eventIds.RemoveAll(x => x == Guid.Empty);
                }

                if(eventIds.Count > 0)
                {
                    for(var i = 0; i < Math.Min(eventIds.Count, messages.Length); i++)
                    {
                        if(eventIds[i] != messages[i].MessageId)
                        {
                            throw new WrongExpectedVersionException(
                                ErrorMessages.AppendFailedWrongExpectedVersion(
                                    streamId.IdOriginal,
                                    StreamVersion.Start),
                                streamId.IdOriginal,
                                StreamVersion.Start);
                        }
                    }

                    if(eventIds.Count < messages.Length && eventIds.Count > 0)
                    {
                        throw new WrongExpectedVersionException(
                            ErrorMessages.AppendFailedWrongExpectedVersion(
                                streamId.IdOriginal,
                                StreamVersion.Start),
                            streamId.IdOriginal,
                            StreamVersion.Start);
                    }

                    cmd.CommandText = @"SELECT [version], [position]
                                        FROM streams
                                        WHERE id_internal = @idInternal;";
                    cmd.Parameters.Clear();
                    cmd.Parameters.AddWithValue("@idInternal", internalId);

                    using(var reader = cmd.ExecuteReader())
                        if(reader.Read())
                        {
                            var ver = reader.ReadScalar<int>(0);
                            var pos = reader.ReadScalar<long>(1);

                            {
                                return new SqliteAppendResult(ver, pos, null);
                            }
                        }
                }
            }

            var stored = StoreMessages(messages, cmd, streamId);
            
            return stored;
        }

        private Task<SqliteAppendResult> AppendToStreamEmpty(SqliteStreamId streamId, NewStreamMessage[] messages, CancellationToken cancellationToken)
        {
            var internalId = ResolveInternalStreamId(streamId.IdOriginal);
            
            using(var conn = OpenConnection(false))
            using(var txn = conn.BeginTransaction())
            using(var cmd = conn.CreateCommand())
            {
                cmd.Transaction = txn;

                var lengthOfStream = GetStreamLength(cmd, internalId, out var result);
                if(lengthOfStream > StreamVersion.Start)
                {
                    throw new WrongExpectedVersionException(
                        ErrorMessages.AppendFailedWrongExpectedVersion(
                            streamId.IdOriginal,
                            StreamVersion.Start),
                        streamId.IdOriginal,
                        StreamVersion.Start);
                }

                var stored = StoreMessages(messages, cmd, streamId);
                
                txn.Commit();

                return Task.FromResult(stored);
            }
        }

        private static int GetStreamLength(SqliteCommand cmd, int? internalId, out SqliteAppendResult appendResult)
        {
            // check to see if the stream has records.  if so, throw wrongexpectedversion exception.
            cmd.CommandText = @"SELECT COUNT(*)
                                    FROM messages
                                    WHERE stream_id_internal = @internalId";
            cmd.Parameters.Clear();
            cmd.Parameters.AddWithValue("@internalId", internalId);
            var streamLength = StreamVersion.End;
            
            if(streamLength > 0)
            {
                // we has information in the stream.
                {
                    appendResult = new SqliteAppendResult(ExpectedVersion.NoStream,
                        ExpectedVersion.NoStream,
                        internalId ?? -1);
                    return streamLength;
                }
            }

            appendResult = default;
            return StreamVersion.Start;
        }

        private Task<SqliteAppendResult> AppendToNonexistentStream(SqliteStreamId streamId, NewStreamMessage[] messages, CancellationToken cancellationToken)
        {
            var internalId = ResolveInternalStreamId(streamId.IdOriginal, throwIfNotExists: false);
            if(internalId != null)
            {
                using(var connection = OpenConnection())
                using(var command = connection.CreateCommand())
                {
                    command.CommandText = @"SELECT event_id
                                    FROM messages
                                    WHERE messages.stream_id_internal = @internalId
                                    ORDER BY messages.position;";
                    command.Parameters.Clear();
                    command.Parameters.AddWithValue("@internalId", internalId);

                    var eventIds = new List<Guid>();
                    using(var reader = command.ExecuteReader())
                    {
                        while(reader.Read())
                        {
                            eventIds.Add(reader.ReadScalar<Guid>(0, Guid.Empty));
                        }

                        eventIds.RemoveAll(x => x == Guid.Empty);
                    }

                    if(eventIds.Count > 0)
                    {
                        for(var i = 0; i < Math.Min(eventIds.Count, messages.Length); i++)
                        {
                            if(eventIds[i] != messages[i].MessageId)
                            {
                                throw new WrongExpectedVersionException(
                                    ErrorMessages.AppendFailedWrongExpectedVersion(
                                        streamId.IdOriginal,
                                        ExpectedVersion.NoStream),
                                    streamId.IdOriginal,
                                    ExpectedVersion.NoStream);
                            }
                        }

                        if(eventIds.Count < messages.Length)
                        {
                            throw new WrongExpectedVersionException(
                                ErrorMessages.AppendFailedWrongExpectedVersion(
                                    streamId.IdOriginal,
                                    ExpectedVersion.NoStream),
                                streamId.IdOriginal,
                                ExpectedVersion.NoStream);
                        }

                        command.CommandText = @"SELECT [version], [position]
                                            FROM streams
                                            WHERE id_internal = @idInternal;";
                        command.Parameters.Clear();
                        command.Parameters.AddWithValue("@idInternal", internalId);

                        using(var reader = command.ExecuteReader())
                            if(reader.Read())
                            {
                                var ver = reader.ReadScalar<int>(0);
                                var pos = reader.ReadScalar<long>(1);

                                {
                                    return Task.FromResult(new SqliteAppendResult(ver, pos, null));
                                }
                            }
                    }
                }
            }

            CreateStream(streamId);
            
            return AppendToStreamEmpty(streamId, messages, cancellationToken);
        }

        private SqliteAppendResult AppendToStreamExpectedVersion(SqliteStreamId streamId, int expectedVersion, NewStreamMessage[] messages, CancellationToken cancellationToken)
        {
            var internalId = ResolveInternalStreamId(streamId.IdOriginal, throwIfNotExists: false);

            if(internalId == null)
            {
                throw new WrongExpectedVersionException(
                    ErrorMessages.AppendFailedWrongExpectedVersion(
                        streamId.IdOriginal,
                        expectedVersion),
                    streamId.IdOriginal,
                    expectedVersion);
            }
            
            using(var connection = OpenConnection(false))
            using(var transaction = connection.BeginTransaction())
            using(var command = connection.CreateCommand())
            {
                command.Transaction = transaction;
                
                if(messages.Length == 1)
                {
                    var msg = messages.First();
                    
                    // tries to fix "When_append_single_message_to_stream_with_correct_expected_version_second_time_with_same_initial_messages_then_should_have_expected_result"
                    command.CommandText = @"SELECT COUNT(*)
                                            FROM messages
                                            WHERE messages.stream_id_internal = @internalId
                                                AND messages.stream_version = @expected
                                                AND messages.event_id = @eventId;";
                    command.Parameters.Clear();
                    command.Parameters.AddWithValue("@internalId", internalId);
                    command.Parameters.AddWithValue("@expected", expectedVersion + 1);
                    command.Parameters.AddWithValue("@eventId", msg.MessageId);
                    var duplicateMessage = command.ExecuteScalar<int?>(0);
                    if(duplicateMessage > 0)
                    {
                        command.CommandText = @"SELECT messages.stream_version, messages.[position]
                                        FROM messages
                                        WHERE messages.stream_id_internal = @stream_id_internal
                                        ORDER BY messages.[position] DESC
                                        LIMIT 1";
                        command.Parameters.Clear();
                        command.Parameters.AddWithValue("@stream_id_internal", internalId ?? -1);
                        using(var reader = command.ExecuteReader())
                        {
                            if (reader.Read() && reader.HasRows)
                                return new SqliteAppendResult(
                                    reader.ReadScalar(0, StreamVersion.End),
                                    reader.ReadScalar(1, Position.End),
                                    internalId ?? -1
                                );
                        }
                    }
                    // end - tries to fix "When_append_single_message_to_stream_with_correct_expected_version_second_time_with_same_initial_messages_then_should_have_expected_result"

                    
                    // tries to fix "When_append_stream_with_expected_version_and_duplicate_message_Id_then_should_throw"
                    command.CommandText = @"SELECT COUNT(*)
                                            FROM messages
                                            WHERE messages.stream_id_internal = @internalId
                                                AND messages.stream_version <= @expected
                                                AND messages.event_id = @eventId;";
                    command.Parameters.Clear();
                    command.Parameters.AddWithValue("@internalId", internalId);
                    command.Parameters.AddWithValue("@expected", expectedVersion);
                    command.Parameters.AddWithValue("@eventId", msg.MessageId);
                    duplicateMessage = command.ExecuteScalar<int?>(0);
                    if(duplicateMessage > 0)
                    {
                        throw new WrongExpectedVersionException(
                            ErrorMessages.AppendFailedWrongExpectedVersion(
                                streamId.IdOriginal,
                                expectedVersion),
                            streamId.IdOriginal,
                            expectedVersion);
                    }
                    // end - tries to fix "When_append_stream_with_expected_version_and_duplicate_message_Id_then_should_throw"
                    
                    command.CommandText = @"SELECT event_id
                            FROM messages
                            WHERE messages.stream_id_internal = @internalId
                                AND messages.stream_version >= @expected
                            ORDER BY messages.position;";
                    command.Parameters.Clear();
                    command.Parameters.AddWithValue("@internalId", internalId);
                    command.Parameters.AddWithValue("@expected", expectedVersion);

                    var eventIds = new List<Guid>();
                    using(var reader = command.ExecuteReader())
                    {
                        while(reader.Read())
                        {
                            eventIds.Add(reader.ReadScalar<Guid>(0, Guid.Empty));
                        }

                        eventIds.RemoveAll(x => x == Guid.Empty);
                    }

                    if(eventIds.Contains(msg.MessageId))
                    {
                        if(eventIds.Count > messages.Length)
                        {
                            throw new WrongExpectedVersionException(
                                ErrorMessages.AppendFailedWrongExpectedVersion(
                                    streamId.IdOriginal,
                                    ExpectedVersion.NoStream),
                                streamId.IdOriginal,
                                ExpectedVersion.NoStream);
                        }

                        command.CommandText = @"SELECT [version], [position]
                                    FROM streams
                                    WHERE id_internal = @idInternal;";
                        command.Parameters.Clear();
                        command.Parameters.AddWithValue("@idInternal", internalId);

                        using(var reader = command.ExecuteReader())
                            if(reader.Read())
                            {
                                var ver = reader.ReadScalar<int>(0);
                                var pos = reader.ReadScalar<long>(1);

                                {
                                    return new SqliteAppendResult(ver, pos, null);
                                }
                            }
                    }
                }

                // does version exist for the stream?
                command.CommandText = @"SELECT streams.[version]
                                    FROM streams
                                    WHERE streams.id_internal = @stream_id_internal;";
                command.Parameters.Clear();
                command.Parameters.AddWithValue("@stream_id_internal", internalId);
                var currentStreamVersion = command.ExecuteScalar<int?>();
                
                if(expectedVersion != currentStreamVersion)
                {
                    var firstMessage = messages.First();
                    
                    // retrieve next series of messages from the first message being requested to
                    var nextMessageIds = new HashSet<Guid>();
                    command.CommandText = @"SELECT messages.event_id
                                FROM messages
                                WHERE messages.stream_id_internal = @stream_id_internal
                                    AND messages.[position] >= (SELECT m.[position] FROM messages m WHERE m.event_id = @event_id) 
                                ORDER BY messages.position
                                LIMIT @message_count;";
                    command.Parameters.Clear();
                    command.Parameters.AddWithValue("@stream_id_internal", internalId);
                    command.Parameters.AddWithValue("@event_id", firstMessage.MessageId);
                    command.Parameters.AddWithValue("@message_count", messages.Length);
                    using(var reader = command.ExecuteReader())
                    while(reader.Read())
                    {
                        nextMessageIds.Add(reader.ReadScalar(0, Guid.Empty));
                    }

                    nextMessageIds.RemoveWhere(msg => msg == Guid.Empty);

                    if(messages.Length != nextMessageIds.Count)
                    {
                        throw new WrongExpectedVersionException(
                            ErrorMessages.AppendFailedWrongExpectedVersion(
                                streamId.IdOriginal,
                                expectedVersion),
                            streamId.IdOriginal,
                            expectedVersion);
                    }

                    // tests for positional inequality between what we know and what is stored. 
                    for(var i = 0; i < Math.Min(messages.Length, nextMessageIds.Count); i++)
                    {
                        var nextMessageId = nextMessageIds.Skip(i).Take(1).SingleOrDefault();
                        if(messages[i].MessageId != nextMessageId)
                        {
                            throw new WrongExpectedVersionException(
                                ErrorMessages.AppendFailedWrongExpectedVersion(
                                    streamId.IdOriginal,
                                    expectedVersion),
                                streamId.IdOriginal,
                                expectedVersion);
                        }
                    }

                    // we seem to be equal.  Query the store to get what would be the last position append result.
                    command.CommandText = @"SELECT messages.stream_version, messages.[position]
                                        FROM messages
                                        WHERE messages.stream_id_internal = @stream_id_internal
                                        ORDER BY messages.[position] DESC
                                        LIMIT 1";
                    command.Parameters.Clear();
                    command.Parameters.AddWithValue("@stream_id_internal", internalId ?? -1);
                    using(var reader = command.ExecuteReader())
                    {
                        if (reader.Read() && reader.HasRows)
                            return new SqliteAppendResult(
                                reader.ReadScalar(0, StreamVersion.End),
                                reader.ReadScalar(1, Position.End),
                                internalId ?? -1
                            );
                    }

                    return new SqliteAppendResult(
                            StreamVersion.End,
                            Position.End,
                            internalId ?? -1
                        );
                }

                var storageResult = StoreMessages(messages, command, streamId);
                
                transaction.Commit();

                return storageResult;
            }
        }

        private int CreateStream(SqliteStreamId streamId, bool throwIfCreateFails = true)
        {
            using (var conn = OpenConnection())
            using (var txn = conn.BeginTransaction())
            using(var cmd = conn.CreateCommand())
            {
                var result = CreateStream(cmd, streamId, throwIfCreateFails);
                txn.Commit();
                return result;
            }        
        }
        
        private int CreateStream(SqliteCommand cmd, SqliteStreamId streamId, bool throwIfCreateFails = true)
        {
            cmd.CommandText = @"SELECT streams.id_internal
                                FROM streams
                                WHERE id_original = @streamId;";
            cmd.Parameters.Clear();
            cmd.Parameters.AddWithValue("@streamId", streamId.IdOriginal);

            var idInternal = cmd.ExecuteScalar<int?>();
            if(idInternal != null)
            {
                return idInternal.Value;
            }
            cmd.CommandText = @"INSERT INTO streams (id, id_original)
                                VALUES (@id, @idOriginal);
                                
                                SELECT last_insert_rowid();";
            cmd.Parameters.AddWithValue("@id", streamId.Id);
            cmd.Parameters.AddWithValue("@idOriginal", streamId.IdOriginal);
            var inserted = cmd.ExecuteScalar<int?>();
            
            if(inserted == null && throwIfCreateFails)
            {
                throw new Exception("Stream failed to create.");
            }

            return inserted ?? int.MinValue;
        }
 
        private SqliteAppendResult StoreMessages(NewStreamMessage[] messages, SqliteCommand cmd, SqliteStreamId streamId)
        {
            // we have to calculate position instead of depending on sqlite to do this for us (using an autoincrement value)
            // because it seems as if we cannot set the initial value to the position being @ zero.
            cmd.CommandText = @"SELECT streams.[position] 
                               FROM streams
                               WHERE streams.id_original = '$position'";
            cmd.Parameters.Clear();
            var position = cmd.ExecuteScalar<long?>();
            if(position == null)
            {
                cmd.CommandText = @"INSERT INTO streams (id, id_original, [version], [position])
                                    VALUES(@id, '$position', 0, -1)";
                cmd.Parameters.Clear();
                cmd.Parameters.AddWithValue("@id", new StreamIdInfo("$position").SqlStreamId.Id);
                cmd.ExecuteScalar();
                position = Position.End;
            }
            
            // resolve stream version, choosing 0 if not exists.
            cmd.CommandText = @"SELECT streams.[version]
                                    FROM streams
                                    WHERE id = @streamId;";
            cmd.Parameters.Clear();
            cmd.Parameters.AddWithValue("@streamId", streamId.Id);
            var version = cmd.ExecuteScalar(StreamVersion.End);

            foreach(var msg in messages)
            {
                cmd.CommandText = @"SELECT COUNT(*)
                                    FROM messages
                                    WHERE event_id = @eventId AND stream_id_internal = (SELECT id_internal FROM streams WHERE id = @streamId);";
                cmd.Parameters.Clear();
                cmd.Parameters.AddWithValue("@streamId", streamId.Id);
                cmd.Parameters.AddWithValue("@eventId", msg.MessageId);
                var hasMessages = (cmd.ExecuteScalar(0) > 0);
                if(hasMessages) continue;

                cmd.CommandText =
                    @"INSERT INTO messages(event_id, stream_id_internal,  [position], stream_version, created_utc, [type], json_data, json_metadata)
                      SELECT               @eventId, streams.id_internal, @position,  @streamVersion, @createdUtc, @type,  @jsonData, @jsonMetadata
                      FROM streams
                      WHERE streams.id = @streamId;
                      UPDATE streams
                      SET version = @streamVersion,
                          position = @position
                      WHERE streams.id = @streamId;";
                // incrementing current version (see above, where it is either set to "StreamVersion.Start", or the value in the db.
                version += 1;
                position += 1;

                cmd.Parameters.Clear();
                cmd.Parameters.AddWithValue("@streamId", streamId.Id);
                cmd.Parameters.AddWithValue("@eventId", msg.MessageId);
                cmd.Parameters.AddWithValue("@position", position);
                cmd.Parameters.AddWithValue("@streamVersion", version);
                cmd.Parameters.AddWithValue("@createdUtc", GetUtcNow());
                cmd.Parameters.AddWithValue("@type", msg.Type);
                cmd.Parameters.AddWithValue("@jsonData", msg.JsonData);
                cmd.Parameters.AddWithValue("@jsonMetadata", msg.JsonMetadata);

                cmd.ExecuteNonQuery();
            }
            
            cmd.CommandText = @"UPDATE streams
                                    SET [version] = @version,
                                        [position] = @position
                                    WHERE id = @streamId;
                                UPDATE streams
                                    SET [position] = @position
                                    WHERE id_original = '$position'";
            cmd.Parameters.Clear();
            cmd.Parameters.AddWithValue("@version", version);
            cmd.Parameters.AddWithValue("@position", position);
            cmd.Parameters.AddWithValue("@streamId", streamId.Id);
            cmd.ExecuteNonQuery();

            
            // need the metadata to build the proper response.
            cmd.CommandText = @"SELECT messages.json_data
                                FROM messages 
                                WHERE messages.position = (
                                    SELECT streams.position
                                    FROM streams
                                    WHERE streams.id_original = @streamId
                                    )";
            cmd.Parameters.Clear();
            cmd.Parameters.AddWithValue("@streamId", $"$${streamId.IdOriginal.Replace("$", "")}");
            var metadataJson = cmd.ExecuteScalar("{}");
            var metadata = string.IsNullOrWhiteSpace(metadataJson)
                ? new MetadataMessage() 
                : SimpleJson.DeserializeObject<MetadataMessage>(metadataJson);
            
            return new SqliteAppendResult(version, position ?? Position.End, metadata.MaxCount);
        }
       
        private async Task CheckStreamMaxCount(
            string streamId, 
            int? maxCount,
            CancellationToken cancellationToken)
        {
            var count = await OpenConnection()
                .Streams(streamId)
                .Length(cancellationToken);
            
            if (count > maxCount)
            {
                int toPurge = count - maxCount.Value;

                var streamMessagesPage = await ReadStreamForwardsInternal(streamId, StreamVersion.Start, toPurge, false, null, cancellationToken)
                    .NotOnCapturedContext();

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
}