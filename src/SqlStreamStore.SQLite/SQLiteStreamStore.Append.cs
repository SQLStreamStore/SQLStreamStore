namespace SqlStreamStore
{
    using System;
    using System.Threading;
    using System.Threading.Tasks;
    using Microsoft.Data.Sqlite;
    using SqlStreamStore.Streams;

    partial class SQLiteStreamStore
    {
        protected override Task<AppendResult> AppendToStreamInternal(
            string streamId, 
            int expectedVersion, 
            NewStreamMessage[] messages, 
            CancellationToken cancellationToken)
        {
            var streamIdInfo = new StreamIdInfo(streamId);

            try
            {
                return Task.FromResult(messages.Length == 0
                    ? CreateEmptyStream(streamIdInfo, expectedVersion, cancellationToken)
                    : AppendMessagesToStream(streamIdInfo.SQLiteStreamId, expectedVersion, messages, cancellationToken)
                );
            }
            catch(SqliteException ex)
            {
                throw new WrongExpectedVersionException(
                    ErrorMessages.AppendFailedWrongExpectedVersion(
                        streamIdInfo.SQLiteStreamId.IdOriginal,
                        expectedVersion),
                    streamIdInfo.SQLiteStreamId.IdOriginal,
                    expectedVersion,
                    ex);
            }
        }

        private AppendResult AppendMessagesToStream(
            SQLiteStreamId streamId, 
            int expectedVersion, 
            NewStreamMessage[] messages, 
            CancellationToken cancellationToken)
        {
            var appendResult = new AppendResult(StreamVersion.End, Position.End);
            var nextExpectedVersion = -1;
            
            using(var connection = OpenConnection())
            using(var transaction = connection.BeginTransaction())
            using(var command = connection.CreateCommand())
            {
                command.Transaction = transaction;
                
                var throwIfAdditionalMessages = false;

                for(var i = 0; i < messages.Length; i++)
                {
                    bool messageExists;
                    (nextExpectedVersion, appendResult, messageExists) = AppendMessageToStream(
                        command,
                        streamId,
                        expectedVersion,
                        messages[i],
                        cancellationToken);

                    if(i == 0)
                    {
                        throwIfAdditionalMessages = messageExists;
                    }
                    else
                    {
                        if(throwIfAdditionalMessages && !messageExists)
                        {
                            throw new WrongExpectedVersionException(
                                ErrorMessages.AppendFailedWrongExpectedVersion(streamId.IdOriginal, expectedVersion),
                                streamId.IdOriginal,
                                expectedVersion
                            );
                        }
                    }

                    expectedVersion = nextExpectedVersion;
                }
                
                transaction.Commit();
            }

            TryScavenge(streamId, cancellationToken);

            return appendResult;
        }

        private (int nextExpectedVersion, AppendResult appendResult, bool messageExists) AppendMessageToStream(
            SqliteCommand command,
            SQLiteStreamId streamId, 
            int expectedVersion, 
            NewStreamMessage message, 
            CancellationToken cancellationToken)
        {
            switch(expectedVersion)
            {
                case ExpectedVersion.Any:
                    return AppendToStreamExpectedVersionAny(command, streamId, message, cancellationToken);
                case ExpectedVersion.NoStream:
                    return AppendToStreamExpectedVersionNoStream(command, streamId, message, cancellationToken);
                case ExpectedVersion.EmptyStream:
                    return AppendToStreamExpectedVersionEmptyStream(command, streamId, message, cancellationToken);
                default:
                    return AppendToStreamExpectedVersion(command, streamId, expectedVersion, message, cancellationToken);
            }
        }

        private (int nextExpectedVersion, AppendResult appendResult, bool messageExists)
            AppendToStreamExpectedVersionAny(
                SqliteCommand command,
                SQLiteStreamId streamId,
                NewStreamMessage message,
                CancellationToken cancellationToken)
        {
            // get internal id.
             long _stream_id_internal;
             command.CommandText = @"SELECT id_internal 
                                     FROM streams 
                                     WHERE id = @streamId
                                     LIMIT 1;";
             command.Parameters.Clear();
             command.Parameters.AddWithValue("@streamId", streamId.Id);
             var internalId = command.ExecuteScalar();

             // build stream if it does not exist.
             if(internalId == null)
             {
                 var metadata = GetStreamMetadata(command, streamId);

                 command.CommandText = @"INSERT INTO streams (id, id_original, max_age, max_count)
                                         VALUES (@id, @idOriginal, @maxAge, @maxCount);
                                         SELECT last_insert_rowid();";
                 command.Parameters.Clear();
                 command.Parameters.AddWithValue("@id", streamId.Id);
                 command.Parameters.AddWithValue("@idOriginal", streamId.IdOriginal);
                 command.Parameters.Add(new SqliteParameter("@maxAge", SqliteType.Integer) { Value = metadata.MaxAge ?? (object)DBNull.Value });
                 command.Parameters.Add(new SqliteParameter("@maxCount", SqliteType.Integer) { Value = metadata.MaxCount ?? (object)DBNull.Value });
                 _stream_id_internal = Convert.ToInt64(command.ExecuteScalar());
             }
             else
             {
                 _stream_id_internal = Convert.ToInt64(internalId);
             }
            
             // determine message count & stream version.
             var messageExists = false;
             var streamVersionBeforeInsert = -1;

             command.CommandText = @"SELECT COUNT(*) > 0, messages.stream_version
                                     FROM messages
                                     WHERE messages.stream_id_internal = @idInternal 
                                       AND messages.message_id = @messageId
                                     LIMIT 1;";
             command.Parameters.Clear();
             command.Parameters.AddWithValue("@idInternal", _stream_id_internal);
             command.Parameters.AddWithValue("@messageId", message.MessageId);
             
             using(var reader = command.ExecuteReader())
             {
                 if(reader.Read())
                 {
                     messageExists = reader.GetBoolean(0);
                     streamVersionBeforeInsert = Convert.ToInt32(reader.IsDBNull(1) ? -1 : reader.GetInt32(1));
                 }
             }

             if(!messageExists)
             {
                 command.CommandText = @"SELECT streams.[version] 
                                         FROM streams 
                                         WHERE id_internal = @idInternal";
                 command.Parameters.Clear();
                 command.Parameters.AddWithValue("@idInternal", _stream_id_internal);
                 streamVersionBeforeInsert = Convert.ToInt32(command.ExecuteScalar());

                 command.CommandText = @"INSERT INTO messages (stream_id_internal, stream_version, message_id, created_utc, type, json_data, json_metadata)
                                         VALUES (@idInternal, @streamVersion + 1, @messageId, @createdUtc, @type, @jsonData, @jsonMetadata);
                                         
                                         SELECT last_insert_rowid();";
                 command.Parameters.Clear();
                 command.Parameters.AddWithValue("@idInternal", _stream_id_internal);
                 command.Parameters.AddWithValue("@streamVersion", streamVersionBeforeInsert);
                 command.Parameters.AddWithValue("@messageId", message.MessageId);
                 command.Parameters.AddWithValue("@createdUtc", GetUtcNow());
                 command.Parameters.AddWithValue("@type", message.Type);
                 command.Parameters.AddWithValue("@jsonData", message.JsonData);
                 command.Parameters.AddWithValue("@jsonMetadata", message.JsonMetadata);
                 var currentPosition = Convert.ToInt64(command.ExecuteScalar());
                 var currentVersion = streamVersionBeforeInsert + 1;

                 command.CommandText = @"UPDATE streams
                                         SET version = @version,
                                             position = @position
                                         WHERE streams.id_internal = @idInternal";
                 command.Parameters.Clear();
                 command.Parameters.AddWithValue("@version", currentVersion);
                 command.Parameters.AddWithValue("@position", currentPosition);
                 command.Parameters.AddWithValue("@idInternal", _stream_id_internal);
                 command.ExecuteNonQuery();

                 return (currentVersion, 
                     new AppendResult(currentVersion, currentPosition),
                     false);
             }
             else
             {
                 command.CommandText = @"SELECT streams.[version], streams.[position]
                                         FROM streams
                                         WHERE streams.id_internal = @idInternal
                                         LIMIT 1";
                 command.Parameters.Clear();
                 command.Parameters.AddWithValue("@idInternal", _stream_id_internal);

                 using(var reader = command.ExecuteReader())
                 {
                     if(reader.Read())
                     {
                         return (streamVersionBeforeInsert, 
                             new AppendResult(reader.GetInt32(0), reader.GetInt64(1)),
                             true);
                     }
                 }
             }
             
             throw new InvalidOperationException("Unhandled case within AppendToStreamExpectedVersionAny.");
        }

        private (int nextExpectedVersion, AppendResult appendResult, bool messageExists) AppendToStreamExpectedVersionNoStream(
            SqliteCommand command,
            SQLiteStreamId streamId, 
            NewStreamMessage message, 
            CancellationToken cancellationToken)
        {
            int currentVersion = 0;
            long currentPosition = 0;
            bool haveMessages = false;

            // get internal id
            long _stream_id_internal = ResolveInternalStream(command, streamId, cancellationToken);

            command.CommandText = @"SELECT COUNT(*) > 0
                FROM messages 
                WHERE messages.stream_id_internal = @streamIdInternal
                    AND messages.message_id = @messageId
                    AND messages.stream_version = 0;";
            command.Parameters.Clear();
            command.Parameters.AddWithValue("@streamIdInternal", _stream_id_internal);
            command.Parameters.AddWithValue("@messageId", message.MessageId);

            var msgCountObject = command.ExecuteScalar();
            haveMessages = Convert.ToBoolean(msgCountObject);

            if(!haveMessages)
            {
                command.CommandText = @"INSERT INTO messages (stream_id_internal, stream_version, message_id, created_utc, [type], json_data, json_metadata)
                                        VALUES(@streamIdInternal, 0, @messageId, @createdUtc, @type, @jsonData, @jsonMetadata);
                                        
                                        SELECT last_insert_rowid();";
                
                command.Parameters.Clear();
                command.Parameters.AddWithValue("@streamIdInternal", _stream_id_internal);
                command.Parameters.AddWithValue("@messageId", message.MessageId);
                command.Parameters.AddWithValue("@createdUtc", GetUtcNow());
                command.Parameters.AddWithValue("@type", message.Type);
                command.Parameters.AddWithValue("@jsonData", message.JsonData);
                command.Parameters.AddWithValue("@jsonMetadata", message.JsonMetadata);

                var position = (long)command.ExecuteScalar();

                command.CommandText = @"UPDATE streams SET [version] = 0, [position] = @position 
                                        WHERE streams.id_internal = @streamIdInternal;";
                command.Parameters.Clear();
                command.Parameters.AddWithValue("@streamIdInternal", _stream_id_internal);
                command.Parameters.AddWithValue("@position", position);

                command.ExecuteNonQuery();
            }

            command.CommandText = @"SELECT [version], [position] 
                                    FROM streams 
                                    WHERE streams.id_internal = @streamIdInternal";
            command.Parameters.Clear();
            command.Parameters.AddWithValue("@streamIdInternal", _stream_id_internal);

            using(var reader = command.ExecuteReader())
            {
                reader.Read();
                currentVersion = reader.GetInt32(0);
                currentPosition = reader.GetInt64(1);
            }

            return (nextExpectedVersion: 0, new AppendResult(currentVersion, currentPosition), haveMessages);
        }

        private (int nextExpectedVersion, AppendResult appendResult, bool messageExists) AppendToStreamExpectedVersionEmptyStream(
            SqliteCommand command,
            SQLiteStreamId streamId, 
            NewStreamMessage message,  
            CancellationToken cancellationToken)
        {
            int currentVersion = 0;
            long currentPosition = 0;
            bool messageExists = false;

            // get internal id
            long _stream_id_internal = ResolveInternalStream(command, streamId, cancellationToken);

            command.CommandText = @"SELECT COUNT(*) > 0 
                FROM messages 
                WHERE messages.stream_id_internal = @streamIdInternal
                    AND messages.message_id = @messageId
                    AND messages.stream_version = 0;";
            command.Parameters.Clear();
            command.Parameters.AddWithValue("@streamIdInternal", _stream_id_internal);
            command.Parameters.AddWithValue("@messageId", message.MessageId);

            messageExists = Convert.ToBoolean(command.ExecuteScalar());

            if(!messageExists)
            {
                command.CommandText = @"INSERT INTO messages (stream_id_internal, stream_version, message_id, created_utc, type, json_data, json_metadata)
                                        VALUES(@streamIdInternal, 0, @messageId, @createdUtc, @type, @jsonData, @jsonMetadata);
                                        
                                        UPDATE streams SET [version] = 0, [position] = last_insert_rowid()
                                        WHERE streams.id_internal = @streamIdInternal;";
                
                command.Parameters.Clear();
                command.Parameters.AddWithValue("@streamIdInternal", _stream_id_internal);
                command.Parameters.AddWithValue("@messageId", message.MessageId);
                command.Parameters.AddWithValue("@createdUtc", GetUtcNow());
                command.Parameters.AddWithValue("@type", message.Type);
                command.Parameters.AddWithValue("@jsonData", message.JsonData);
                command.Parameters.AddWithValue("@jsonMetadata", message.JsonMetadata);

                command.ExecuteNonQuery();
            }

            command.CommandText = @"SELECT 0, messages.position 
                                    FROM messages 
                                    WHERE messages.message_id = @messageId 
                                        AND messages.stream_id_internal = @streamIdInternal;";
            command.Parameters.Clear();
            command.Parameters.AddWithValue("@messageId", message.MessageId);
            command.Parameters.AddWithValue("@streamIdInternal", _stream_id_internal);

            using(var reader = command.ExecuteReader())
            {
                reader.Read();
                currentVersion = reader.GetInt32(0);
                currentPosition = reader.GetInt64(1);
            }

            return (nextExpectedVersion: 0, new AppendResult(currentVersion, currentPosition), messageExists);
        }

        private (int nextExpectedVersion, AppendResult appendResult, bool messageExists) AppendToStreamExpectedVersion(
            SqliteCommand command,
            SQLiteStreamId streamId, 
            int expectedVersion, 
            NewStreamMessage message, 
            CancellationToken cancellationToken)
        {
            int currentVersion = 0;
            long currentPosition = 0;
            bool messageExists = false;
            
            // get internal id
            command.CommandText = @"SELECT streams.id_internal
                                    FROM streams
                                    WHERE streams.id = @streamId";
            command.Parameters.Clear();
            command.Parameters.AddWithValue("@streamId", streamId.Id);
            var streamIdInternalObject = command.ExecuteScalar();
            if(streamIdInternalObject == null || streamIdInternalObject == DBNull.Value)
            {
                throw new WrongExpectedVersionException(
                    ErrorMessages.AppendFailedWrongExpectedVersion(streamId.Id, expectedVersion),
                    streamId.Id,
                    expectedVersion);
            }

            var _stream_id_internal = Convert.ToInt64(streamIdInternalObject);

            command.CommandText = @"SELECT streams.version 
                                    FROM streams 
                                    WHERE streams.id_internal = @streamIdInternal;";
            command.Parameters.Clear();
            command.Parameters.AddWithValue("@streamIdInternal", _stream_id_internal);

            var internalStreamVersion = (long)command.ExecuteScalar();
            if(internalStreamVersion < expectedVersion)
            {
                throw new WrongExpectedVersionException(
                    ErrorMessages.AppendFailedWrongExpectedVersion(streamId.Id, expectedVersion),
                    streamId.Id,
                    expectedVersion);
            }
            
            command.CommandText = @"SELECT COUNT(*) 
                FROM messages 
                WHERE messages.stream_id_internal = @streamIdInternal
                    AND messages.stream_version = @expectedVersion
                    AND messages.message_id = @messageId;";
            command.Parameters.Clear();
            command.Parameters.AddWithValue("@streamIdInternal", _stream_id_internal);
            command.Parameters.AddWithValue("@expectedVersion", expectedVersion + 1);
            command.Parameters.AddWithValue("@messageId", message.MessageId);

            var messagesCountObj = command.ExecuteScalar();
            var messagesCount = Convert.ToInt64(messagesCountObj);
            messageExists = messagesCount > 0;

            if(!messageExists)
            {
                command.CommandText = @"INSERT INTO messages (stream_id_internal, stream_version, message_id, created_utc, type, json_data, json_metadata)
                                        VALUES(@streamIdInternal, @expectedVersion + 1, @messageId, @createdUtc, @type, @jsonData, @jsonMetadata);
                                        
                                        UPDATE streams SET [version] = @expectedVersion + 1, [position] = last_insert_rowid()
                                        WHERE streams.id_internal = @streamIdInternal;
                                        
                                        SELECT [position]
                                        FROM streams
                                        WHERE id_internal = @streamIdInternal;";
                
                command.Parameters.Clear();
                command.Parameters.AddWithValue("@streamIdInternal", _stream_id_internal);
                command.Parameters.AddWithValue("@expectedVersion", expectedVersion);
                command.Parameters.AddWithValue("@messageId", message.MessageId);
                command.Parameters.AddWithValue("@createdUtc", GetUtcNow());
                command.Parameters.AddWithValue("@type", message.Type);
                command.Parameters.AddWithValue("@jsonData", message.JsonData);
                command.Parameters.AddWithValue("@jsonMetadata", message.JsonMetadata);
                
                currentPosition = Convert.ToInt64(command.ExecuteScalar());
                currentVersion = expectedVersion + 1;
            }
            else
            {
                command.CommandText = @"SELECT streams.version, streams.position
                                FROM streams WHERE streams.id_internal = @streamIdInternal;";
                command.Parameters.Clear();
                command.Parameters.AddWithValue("@streamIdInternal", _stream_id_internal);

                using(var reader = command.ExecuteReader())
                    if(reader.Read())
                    {
                        currentVersion = reader.GetInt32(0);
                        currentPosition = reader.GetInt64(1);
                    }
            }

            return (nextExpectedVersion: expectedVersion + 1, 
                new AppendResult(currentVersion, currentPosition), 
                messageExists);
        }

        private AppendResult CreateEmptyStream(
            StreamIdInfo streamIdInfo,
            int expectedVersion,
            CancellationToken cancellationToken)
        {
            var appendResult = new AppendResult(StreamVersion.End, Position.End);
            
            using(var connection = OpenConnection())
            using(var transaction = connection.BeginTransaction())
            using (var command = new SqliteCommand("", connection, transaction))
            {
                if(expectedVersion == ExpectedVersion.EmptyStream)
                {
                    command.CommandText = "SELECT streams.version >= 0 FROM streams WHERE streams.id = @streamId";
                    command.Parameters.Clear();
                    command.Parameters.AddWithValue("@streamId", streamIdInfo.SQLiteStreamId.Id);

                    if(Convert.ToBoolean(command.ExecuteScalar()))
                    {
                        throw new WrongExpectedVersionException(
                            ErrorMessages.AppendFailedWrongExpectedVersion(
                                streamIdInfo.SQLiteStreamId.IdOriginal, 
                                expectedVersion),
                            streamIdInfo.SQLiteStreamId.IdOriginal,
                            expectedVersion);
                    }
                }
                else if(expectedVersion == ExpectedVersion.NoStream)
                {
                    command.CommandText = "SELECT COUNT(*) FROM streams WHERE streams.id = @streamId";
                    command.Parameters.Clear();
                    command.Parameters.AddWithValue("@streamId", streamIdInfo.SQLiteStreamId.Id);
                    var streamCount = Convert.ToInt32(command.ExecuteScalar());

                    if(streamCount > 0)
                    {
                        throw new WrongExpectedVersionException(
                            ErrorMessages.AppendFailedWrongExpectedVersion(
                                streamIdInfo.SQLiteStreamId.IdOriginal, 
                                expectedVersion),
                            streamIdInfo.SQLiteStreamId.IdOriginal,
                            expectedVersion);
                    }
                }
                
                command.CommandText = "SELECT COUNT(*) FROM streams WHERE streams.id = @streamId";
                command.Parameters.Clear();
                command.Parameters.AddWithValue("@streamId", streamIdInfo.SQLiteStreamId.Id);
                var numberOfStreams = Convert.ToInt32(command.ExecuteScalar());

                if(numberOfStreams == 0)
                {
                    var maxAgeCount = GetStreamMetadata(command, streamIdInfo.SQLiteStreamId);
                    
                    command.CommandText = @"INSERT INTO streams (id, id_original, max_age, max_count)
                                            VALUES(@streamId, @streamIdOriginal, @maxAge, @maxCount);
                                            SELECT last_insert_rowid();";
                    command.Parameters.Clear();
                    command.Parameters.AddWithValue("@streamId", streamIdInfo.SQLiteStreamId.Id);
                    command.Parameters.AddWithValue("@streamIdOriginal", streamIdInfo.SQLiteStreamId.IdOriginal);
                    command.Parameters.AddWithValue("@maxAge", maxAgeCount.MaxAge ?? (object)DBNull.Value);
                    command.Parameters.AddWithValue("@maxCount", maxAgeCount.MaxCount ?? (object)DBNull.Value);

                    command.ExecuteNonQuery();
                }

                command.CommandText = @"SELECT streams.version, streams.position 
                                        FROM streams 
                                        WHERE streams.id = @streamId";
                command.Parameters.Clear();
                command.Parameters.AddWithValue("@streamId", streamIdInfo.SQLiteStreamId.Id);

                using(var reader = command.ExecuteReader())
                {
                    reader.Read();
                    appendResult = new AppendResult(reader.GetInt32(0), reader.GetInt64(1));
                }
                
                transaction.Commit();
            }

            return appendResult;
        }

        private long ResolveInternalStream(SqliteCommand command, SQLiteStreamId streamId, CancellationToken cancellationToken)
        {
            // get internal id
            long _stream_id_internal;
            command.CommandText = "SELECT id_internal FROM streams WHERE id = @streamId";
            command.Parameters.Clear();
            command.Parameters.AddWithValue("@streamId", streamId.Id);
            var result = command.ExecuteScalar();
            if(result == DBNull.Value || result == null)
            {
                command.CommandText = @"INSERT INTO streams (id, id_original, max_age, max_count)
                                            VALUES(@streamId, @streamIdOriginal, @maxAge, @maxCount);
                                            SELECT last_insert_rowid();";
                command.Parameters.Clear();
                command.Parameters.AddWithValue("@streamId", streamId.Id);
                command.Parameters.AddWithValue("@streamIdOriginal", streamId.IdOriginal);
                command.Parameters.AddWithValue("@maxAge", DBNull.Value);
                command.Parameters.AddWithValue("@maxCount", DBNull.Value);
                _stream_id_internal = Convert.ToInt64(command.ExecuteScalar());
            }
            else
            {
                _stream_id_internal = Convert.ToInt64(result);
            }

            return _stream_id_internal;
        }

        private (int? MaxAge, int? MaxCount) GetStreamMetadata(SqliteCommand command, SQLiteStreamId streamId)
        {
            // get internal id
            long _stream_id_internal;
            var maxAge = default(int?);
            var maxCount = default(int?);
            
            command.CommandText = "SELECT id_internal FROM streams WHERE id = @streamId";
            command.Parameters.Clear();
            command.Parameters.AddWithValue("@streamId", streamId.Id);
            var result = command.ExecuteScalar();

            if(result != DBNull.Value && result != null)
            {
                _stream_id_internal = Convert.ToInt64(result);
                
                command.CommandText = @"SELECT messages.json_metadata 
                                        FROM messages 
                                        WHERE messages.stream_id_internal = @streamIdInternal 
                                        ORDER BY messages.stream_version DESC 
                                        LIMIT 1;";
                command.Parameters.Clear();
                command.Parameters.AddWithValue("@streamIdInternal", _stream_id_internal);
                
                var jsonDataObject = command.ExecuteScalar();
                if(jsonDataObject != DBNull.Value && jsonDataObject != null)
                {
                    var jsonData = jsonDataObject.ToString();

                    var idx = jsonData.IndexOf("\"MaxAge\":");
                    if(idx > -1)
                    {
                        var startIndex = idx + 9;
                        var length = jsonData.IndexOf(',', startIndex + 1);
                        var metadataValue = jsonData.Substring(startIndex, length - startIndex);
                    
                        maxAge = (metadataValue == "null" || metadataValue == "") 
                            ? (int?)null 
                            : Convert.ToInt32(metadataValue);
                    }

                    idx = jsonData.IndexOf("\"MaxCount\":");
                    if(idx > -1)
                    {
                        var startIndex = idx + 11;
                        var length = jsonData.IndexOf(',', startIndex + 1);
                        var metadataValue = jsonData.Substring(startIndex, length- startIndex);

                        maxCount = (metadataValue == "null" || metadataValue == "")
                            ? (int?) null
                            : Convert.ToInt32(metadataValue);
                    }
                }
            }

            return (maxAge, maxCount);
        }

        internal class StreamMeta
        {
            public static readonly StreamMeta None = new StreamMeta(null, null);

            public StreamMeta(int? maxCount, int? maxAge)
            {
                MaxCount = maxCount;
                MaxAge = maxAge;
            }

            public int? MaxCount { get; }

            public int? MaxAge { get; }
        }
    }
}