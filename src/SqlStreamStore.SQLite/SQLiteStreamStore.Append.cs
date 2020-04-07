namespace SqlStreamStore
{
    using System;
    using System.Threading;
    using System.Threading.Tasks;
    using Microsoft.Data.Sqlite;
    using SqlStreamStore.Imports.Ensure.That;
    using SqlStreamStore.Streams;

    partial class SQLiteStreamStore
    {
        protected override Task<AppendResult> AppendToStreamInternal(
            string streamId, 
            int expectedVersion, 
            NewStreamMessage[] messages, 
            CancellationToken cancellationToken)
        {
            Ensure.That(streamId, "streamId").IsNotNullOrWhiteSpace();
            Ensure.That(expectedVersion, "expectedVersion").IsGte(ExpectedVersion.NoStream);
            Ensure.That(messages, "Messages").IsNotNull();
            GuardAgainstDisposed();

            SQLiteAppendResult result;
            using(var connection = OpenConnection())
            using(var transaction = connection.BeginTransaction())
            using(var command = connection.CreateCommand())
            {
                command.Transaction = transaction;
                result = AppendToStreamInternal(command, streamId, expectedVersion, messages, cancellationToken)
                    .GetAwaiter().GetResult();
                transaction.Commit();
            }

            if(result.MaxCount.HasValue)
            {
                CheckStreamMaxCount(streamId, result.MaxCount, cancellationToken).Wait(cancellationToken);
            }
            
            
            return Task.FromResult(new AppendResult(result.CurrentVersion, result.CurrentPosition));
        }

        private Task<SQLiteAppendResult> AppendToStreamInternal(
            SqliteCommand command,
            string streamId,
            int expectedVersion,
            NewStreamMessage[] messages,
            CancellationToken cancellationToken)
        {
            try
            {
                return Task.FromResult(messages.Length == 0
                    ? CreateEmptyStream(command, streamId, expectedVersion, cancellationToken)
                    : AppendMessagesToStream(command,
                        streamId,
                        expectedVersion,
                        messages,
                        cancellationToken));
            }
            catch(SqliteException ex)
            {
                throw new WrongExpectedVersionException(
                    ErrorMessages.AppendFailedWrongExpectedVersion(
                        streamId,
                        expectedVersion),
                    streamId,
                    expectedVersion,
                    ex);
            }
        }

        private SQLiteAppendResult AppendMessagesToStream(
            SqliteCommand command,
            string streamId, 
            int expectedVersion, 
            NewStreamMessage[] messages, 
            CancellationToken cancellationToken)
        {
            var appendResult = new SQLiteAppendResult();
            var nextExpectedVersion = -1;
            var messageExists = false;
            
            var throwIfAdditionalMessages = false;

            for(var i = 0; i < messages.Length; i++)
            {
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
                            ErrorMessages.AppendFailedWrongExpectedVersion(streamId, expectedVersion),
                            streamId,
                            expectedVersion
                        );
                    }
                }

                expectedVersion = nextExpectedVersion;
            }
            
            return appendResult;
        }

        private (int nextExpectedVersion, SQLiteAppendResult appendResult, bool messageExists) AppendMessageToStream(
            SqliteCommand command,
            string streamId, 
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

        private (int nextExpectedVersion, SQLiteAppendResult appendResult, bool messageExists)
            AppendToStreamExpectedVersionAny(
                SqliteCommand command,
                string streamId,
                NewStreamMessage message,
                CancellationToken cancellationToken)
        {
            var s = new SQLiteStreamId(streamId);
            // get internal id.
             command.CommandText = @"SELECT id_internal 
                                     FROM streams 
                                     WHERE id = @streamId
                                     LIMIT 1;";
             command.Parameters.Clear();
             command.Parameters.AddWithValue("@streamId", s.Id);
             var _stream_id_internal= command.ExecuteScalar<long?>();
             (int? MaxAge, int? MaxCount) streamMetadata = GetStreamMetadata(command, s);

             // build stream if it does not exist.
             if(_stream_id_internal == null)
             {
                 command.CommandText = @"INSERT INTO streams (id, id_original, max_age, max_count)
                                         VALUES (@id, @idOriginal, @maxAge, @maxCount);
                                         SELECT last_insert_rowid();";
                 command.Parameters.Clear();
                 command.Parameters.AddWithValue("@id", s.Id);
                 command.Parameters.AddWithValue("@idOriginal", s.IdOriginal);
                 command.Parameters.Add(new SqliteParameter("@maxAge", SqliteType.Integer) { Value = streamMetadata.MaxAge ?? (object)DBNull.Value });
                 command.Parameters.Add(new SqliteParameter("@maxCount", SqliteType.Integer) { Value = streamMetadata.MaxCount ?? (object)DBNull.Value });
                 _stream_id_internal = command.ExecuteScalar<long?>();
             }
            
             // // determine message count & stream version.
             // int? streamVersionBeforeInsert = -1;
             //
             // command.CommandText = @"SELECT COUNT(*) > 0
             //                         FROM messages 
             //                         WHERE messages.stream_id_internal = @idInternal;";
             // command.Parameters.Clear();
             // command.Parameters.AddWithValue("@idInternal", _stream_id_internal);
             // var messageExists = (command.ExecuteScalar<long?>() ?? 0) > 0;
             //
             // if(!messageExists)
             // {
                 command.CommandText = @"SELECT streams.[version] 
                                         FROM streams 
                                         WHERE id_internal = @idInternal";
                 command.Parameters.Clear();
                 command.Parameters.AddWithValue("@idInternal", _stream_id_internal);
                 var newEventStreamVersion = Convert.ToInt32(command.ExecuteScalar<long?>());

                 command.CommandText = @"INSERT INTO messages (stream_id_internal, stream_version, message_id, created_utc, type, json_data, json_metadata)
                                         VALUES (@idInternal, @streamVersion + 1, @messageId, @createdUtc, @type, @jsonData, @jsonMetadata);
                                         
                                         SELECT last_insert_rowid();";
                 command.Parameters.Clear();
                 command.Parameters.AddWithValue("@idInternal", _stream_id_internal);
                 command.Parameters.AddWithValue("@streamVersion", newEventStreamVersion);
                 command.Parameters.AddWithValue("@messageId", message.MessageId);
                 command.Parameters.AddWithValue("@createdUtc", GetUtcNow());
                 command.Parameters.AddWithValue("@type", message.Type);
                 command.Parameters.AddWithValue("@jsonData", message.JsonData);
                 command.Parameters.AddWithValue("@jsonMetadata", message.JsonMetadata);
                 var currentPosition = command.ExecuteScalar<long?>();
                 var currentVersion = newEventStreamVersion + 1;

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
                     new SQLiteAppendResult(streamMetadata.MaxCount, currentVersion, currentPosition ?? -1), 
                     false);
             // }
             //
             //
             // command.CommandText = @"SELECT streams.[version], streams.[position]
             //                         FROM streams
             //                         WHERE streams.id_internal = @idInternal
             //                         LIMIT 1";
             // command.Parameters.Clear();
             // command.Parameters.AddWithValue("@idInternal", _stream_id_internal);
             //
             // using(var reader = command.ExecuteReader())
             // {
             //     if(reader.Read())
             //     {
             //         return (streamVersionBeforeInsert ?? -1, 
             //             new SQLiteAppendResult(streamMetadata.MaxCount, reader.GetInt32(0), reader.GetInt64(1)), 
             //             messageExists);
             //     }
             // }
             //
             //
             // throw new InvalidOperationException("Unhandled case within AppendToStreamExpectedVersionAny.");
        }

        private (int nextExpectedVersion, SQLiteAppendResult appendResult, bool messageExists) AppendToStreamExpectedVersionNoStream(
            SqliteCommand command,
            string streamId, 
            NewStreamMessage message, 
            CancellationToken cancellationToken)
        {
            int currentVersion = 0;
            long currentPosition = 0;
            var s = new SQLiteStreamId(streamId);

            // get internal id
            long _stream_id_internal = ResolveInternalStream(command, s, cancellationToken);
            command.CommandText = @"SELECT COUNT(*) > 0
                FROM messages 
                WHERE messages.stream_id_internal = @streamIdInternal
                    AND messages.message_id = @messageId
                    AND messages.stream_version = 0;";
            command.Parameters.Clear();
            command.Parameters.AddWithValue("@streamIdInternal", _stream_id_internal);
            command.Parameters.AddWithValue("@messageId", message.MessageId);
            bool haveMessages = (command.ExecuteScalar<long?>() ?? 0) > 0;

            // if we do not have any messages for the provided stream
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

                var position = command.ExecuteScalar<long?>();

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

            return (nextExpectedVersion: 0, new SQLiteAppendResult(null, currentVersion, currentPosition), haveMessages);
        }

        private (int nextExpectedVersion, SQLiteAppendResult appendResult, bool messageExists) AppendToStreamExpectedVersionEmptyStream(
            SqliteCommand command,
            string streamId, 
            NewStreamMessage message,  
            CancellationToken cancellationToken)
        {
            int currentVersion = 0;
            long currentPosition = 0;
            var s = new SQLiteStreamId(streamId);

            // get internal id
            command.CommandText = "SELECT id_internal FROM streams WHERE id = @streamId";
            command.Parameters.Clear();
            command.Parameters.AddWithValue("@streamId", s.Id);
            var _stream_id_internal = command.ExecuteScalar<long?>();
            if(_stream_id_internal == null)
            {
                return (ExpectedVersion.NoStream, 
                    new SQLiteAppendResult(null, ExpectedVersion.NoStream, ExpectedVersion.NoStream), 
                    false);
            }
            
            command.CommandText = @"SELECT COUNT(*) > 0 
                FROM messages 
                WHERE messages.stream_id_internal = @streamIdInternal
                    AND messages.message_id = @messageId
                    AND messages.stream_version = 0;";
            command.Parameters.Clear();
            command.Parameters.AddWithValue("@streamIdInternal", _stream_id_internal);
            command.Parameters.AddWithValue("@messageId", message.MessageId);
            var messageExists = (command.ExecuteScalar<long?>() ?? 0) > 0;

            // if we do not have any messages for the provided stream
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

            return (nextExpectedVersion: 0, new SQLiteAppendResult(null, currentVersion, currentPosition), messageExists);
        }

        private (int nextExpectedVersion, SQLiteAppendResult appendResult, bool messageExists) AppendToStreamExpectedVersion(
            SqliteCommand command,
            string streamId, 
            int expectedVersion, 
            NewStreamMessage message, 
            CancellationToken cancellationToken)
        {
            int? currentVersion = 0;
            long? currentPosition = 0;
            var s= new SQLiteStreamId(streamId);
            
            // get internal id
            command.CommandText = @"SELECT streams.id_internal
                                    FROM streams
                                    WHERE streams.id = @streamId";
            command.Parameters.Clear();
            command.Parameters.AddWithValue("@streamId", s.Id);
            var _stream_id_internal = command.ExecuteScalar<long?>();
            
            if(_stream_id_internal == null)
            {
                throw new WrongExpectedVersionException(
                    ErrorMessages.AppendFailedWrongExpectedVersion(s.Id, expectedVersion),
                    s.Id,
                    expectedVersion);
            }

            command.CommandText = @"SELECT streams.version 
                                    FROM streams 
                                    WHERE streams.id_internal = @streamIdInternal;";
            command.Parameters.Clear();
            command.Parameters.AddWithValue("@streamIdInternal", _stream_id_internal);

            var internalStreamVersion = command.ExecuteScalar<long?>();
            if(internalStreamVersion < expectedVersion)
            {
                throw new WrongExpectedVersionException(
                    ErrorMessages.AppendFailedWrongExpectedVersion(s.Id, expectedVersion),
                    s.Id,
                    expectedVersion);
            }
            
            command.CommandText = @"SELECT COUNT(*) > 0
                FROM messages 
                WHERE messages.stream_id_internal = @streamIdInternal
                    AND messages.stream_version = @expectedVersion
                    AND messages.message_id = @messageId;";
            command.Parameters.Clear();
            command.Parameters.AddWithValue("@streamIdInternal", _stream_id_internal);
            command.Parameters.AddWithValue("@expectedVersion", expectedVersion + 1);
            command.Parameters.AddWithValue("@messageId", message.MessageId);
            var messageExists = (command.ExecuteScalar<long?>() ?? 0) > 0;

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
                
                currentPosition = command.ExecuteScalar<long?>();
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
                new SQLiteAppendResult(null, currentVersion ?? 0, currentPosition ?? 0), 
                messageExists);
        }

        private SQLiteAppendResult CreateEmptyStream(
            SqliteCommand command,
            string streamId,
            int expectedVersion,
            CancellationToken cancellationToken)
        {
            var s = new SQLiteStreamId(streamId);

            if(expectedVersion == ExpectedVersion.EmptyStream)
            {
                command.CommandText = "SELECT streams.version >= 0 FROM streams WHERE streams.id = @streamId";
                command.Parameters.Clear();
                command.Parameters.AddWithValue("@streamIdOriginal", s.Id);

                if(!command.ExecuteScalar<bool?>() ?? false)
                {
                    throw new WrongExpectedVersionException(
                        ErrorMessages.AppendFailedWrongExpectedVersion(
                            streamId, 
                            expectedVersion),
                        streamId,
                        expectedVersion);
                }
            }
            else if(expectedVersion == ExpectedVersion.NoStream)
            {
                command.CommandText = "SELECT COUNT(*) FROM streams WHERE streams.id = @streamId";
                command.Parameters.Clear();
                command.Parameters.AddWithValue("@streamId", s.Id);
                var streamCount = command.ExecuteScalar<long?>();

                if(streamCount > 0)
                {
                    throw new WrongExpectedVersionException(
                        ErrorMessages.AppendFailedWrongExpectedVersion(
                            streamId, 
                            expectedVersion),
                        streamId,
                        expectedVersion);
                }
            }
            
            command.CommandText = "SELECT COUNT(*) FROM streams WHERE streams.id = @streamId";
            command.Parameters.Clear();
            command.Parameters.AddWithValue("@streamId", s.Id);
            var numberOfStreams = command.ExecuteScalar<long?>();
            var maxAgeCount = GetStreamMetadata(command, s);

            if(numberOfStreams == 0)
            {
                command.CommandText = @"INSERT INTO streams (id, id_original, max_age, max_count)
                                        VALUES(@streamId, @streamIdOriginal, @maxAge, @maxCount);
                                        SELECT last_insert_rowid();";
                command.Parameters.Clear();
                command.Parameters.AddWithValue("@streamId", s.Id);
                command.Parameters.AddWithValue("@streamIdOriginal", s.IdOriginal);
                command.Parameters.AddWithValue("@maxAge", maxAgeCount.MaxAge ?? (object)DBNull.Value);
                command.Parameters.AddWithValue("@maxCount", maxAgeCount.MaxCount ?? (object)DBNull.Value);

                command.ExecuteNonQuery();
            }

            command.CommandText = @"SELECT streams.version, streams.position 
                                    FROM streams 
                                    WHERE streams.id = @streamId";
            command.Parameters.Clear();
            command.Parameters.AddWithValue("@streamId", s.Id);

            using(var reader = command.ExecuteReader())
            {
                reader.Read();
                return new SQLiteAppendResult(
                    maxAgeCount.MaxCount,
                    reader.GetValue(0) == DBNull.Value ? 0 : reader.GetInt32(0),
                    reader.GetValue(1) == DBNull.Value ? 0 : reader.GetInt64(1)
                );
            }
        }

        private long ResolveInternalStream(SqliteCommand command, SQLiteStreamId streamId, CancellationToken cancellationToken)
        {
            // get internal id
            command.CommandText = "SELECT id_internal FROM streams WHERE id = @streamId";
            command.Parameters.Clear();
            command.Parameters.AddWithValue("@streamId", streamId.Id);
            var _stream_id_internal = command.ExecuteScalar<long?>();
            
            if(_stream_id_internal == null)
            {
                command.CommandText = @"INSERT INTO streams (id, id_original, max_age, max_count)
                                            VALUES(@streamId, @streamIdOriginal, @maxAge, @maxCount);
                                            SELECT last_insert_rowid();";
                command.Parameters.Clear();
                command.Parameters.AddWithValue("@streamId", streamId.Id);
                command.Parameters.AddWithValue("@streamIdOriginal", streamId.IdOriginal);
                command.Parameters.AddWithValue("@maxAge", DBNull.Value);
                command.Parameters.AddWithValue("@maxCount", DBNull.Value);
                _stream_id_internal = command.ExecuteScalar<long?>();
            }
            
            return _stream_id_internal ?? ExpectedVersion.NoStream;
        }

        private (int? MaxAge, int? MaxCount) GetStreamMetadata(SqliteCommand command, SQLiteStreamId streamId)
        {
            // get internal id
            var maxAge = default(int?);
            var maxCount = default(int?);
            
            command.CommandText = "SELECT id_internal FROM streams WHERE id_original = @streamIdOriginal";
            command.Parameters.Clear();
            command.Parameters.AddWithValue("@streamIdOriginal", streamId.IdOriginal);
            var _stream_id_internal = command.ExecuteScalar<long?>();

            if(_stream_id_internal != null)
            {
                command.CommandText = @"SELECT messages.json_metadata 
                                        FROM messages 
                                        WHERE messages.stream_id_internal = @streamIdInternal 
                                        ORDER BY messages.stream_version DESC 
                                        LIMIT 1;";
                command.Parameters.Clear();
                command.Parameters.AddWithValue("@streamIdInternal", _stream_id_internal);
                
                var jsonData = command.ExecuteScalar<string>();
                if(!string.IsNullOrWhiteSpace(jsonData))
                {
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
        
        private async Task CheckStreamMaxCount(string streamId, int? maxCount, CancellationToken cancellationToken)
        {
            if (maxCount.HasValue)
            {
                var count = await GetStreamMessageCount(streamId, cancellationToken);
                if (count > maxCount.Value)
                {
                    int toPurge = count - maxCount.Value;

                    var streamMessagesPage = await ReadStreamForwardsInternal(streamId, StreamVersion.Start,
                        toPurge, false, null, cancellationToken);

                    if (streamMessagesPage.Status == PageReadStatus.Success)
                    {
                        foreach (var message in streamMessagesPage.Messages)
                        {
                            await DeleteEventInternal(streamId, message.MessageId, cancellationToken);
                        }
                    }
                }
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
                                        WHERE streams.id_original = @streamIdOriginal";
                var streamIdInfo = new StreamIdInfo(streamId);
                command.Parameters.AddWithValue("@streamIdOriginal", streamIdInfo.SQLiteStreamId.IdOriginal);

                return Task.FromResult(command.ExecuteScalar<int>());
            }
        }
    }
}