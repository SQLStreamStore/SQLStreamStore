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
                    : AppendMessagesToStream(streamIdInfo, expectedVersion, messages, cancellationToken)
                );
            }
            catch(Exception e)
            {
                Console.WriteLine(e);
                throw;
            }
        }

        private AppendResult AppendMessagesToStream(
            StreamIdInfo streamId, 
            int expectedVersion, 
            NewStreamMessage[] messages, 
            CancellationToken cancellationToken)
        {
            var appendResult = new AppendResult(StreamVersion.End, Position.End);
            var nextExpectedVersion = expectedVersion;
            
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
                        streamId.SQLiteStreamId,
                        nextExpectedVersion,
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
                                ErrorMessages.AppendFailedWrongExpectedVersion(streamId.SQLiteStreamId.IdOriginal, expectedVersion),
                                streamId.SQLiteStreamId.IdOriginal,
                                expectedVersion
                            );
                        }
                    }
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

        private (int nextExpectedVersion, AppendResult appendResult, bool messageExists) AppendToStreamExpectedVersionAny(
            SqliteCommand command,
            SQLiteStreamId streamId, 
            NewStreamMessage message, 
            CancellationToken cancellationToken)
        {
                // get internal id
                long _stream_id_internal = ResolveInternalStream(command, streamId, cancellationToken);

                command.CommandText = @"SELECT COUNT(*) > 0, stream_version
FROM messages
WHERE messages.stream_id_internal = @streamIdInternal
    AND messages.message_id = @messageId";
                command.Parameters.Clear();
                command.Parameters.AddWithValue("@streamIdInternal", _stream_id_internal);
                command.Parameters.AddWithValue("@messageId", message.MessageId);

                bool messageExists = false;
                int streamVersion = -1;
                var streamPosition = -1;
                
                using(var reader = command.ExecuteReader())
                {
                    if(reader.Read())
                    {
                        messageExists = reader.GetBoolean(0);
                        streamVersion = messageExists ? reader.GetInt32(1) : -1;
                    }
                }

                if(!messageExists)
                {
                    command.CommandText = @"INSERT INTO messages(stream_id_internal, stream_version, message_id, created_utc, type, json_data, json_metadata)
                    SELECT @streamIdInternal, @streamVersion, @messageId, @createdUtc, @type, @jsonData, @jsonMetadata
                    FROM streams
                    WHERE id_internal = @streamIdInternal;
                    
                    UPDATE streams
                    SET [version] = @streamVersion,
                        [position] = last_insert_rowid()
                    WHERE streams.id_internal = @streamIdInternal;";
                    command.Parameters.Clear();
                    command.Parameters.AddWithValue("@streamIdInternal", _stream_id_internal);
                    command.Parameters.AddWithValue("@streamVersion", streamVersion + 1);
                    command.Parameters.AddWithValue("@messageId", message.MessageId);
                    command.Parameters.AddWithValue("@createdUtc", GetUtcNow());
                    command.Parameters.AddWithValue("@type", message.Type);
                    command.Parameters.AddWithValue("@jsonData", message.JsonData);
                    command.Parameters.AddWithValue("@jsonMetadata", message.JsonMetadata);
                    command.ExecuteNonQuery();
                }
                else
                {
                    command.CommandText = @"SELECT streams.version, streams.position 
FROM streams 
WHERE streams.id_internal = @streamIdInternal;";
                    command.Parameters.Clear();
                    command.Parameters.AddWithValue("@streamIdInternal", _stream_id_internal);

                    using(var reader = command.ExecuteReader())
                    {
                        reader.Read();
                        streamVersion = reader.GetInt32(0);
                        streamPosition = reader.GetInt32(1);
                    }
                }
                
                return (streamVersion, 
                    new AppendResult(streamVersion + 1, streamPosition), 
                    messageExists);

        }

        private (int nextExpectedVersion, AppendResult appendResult, bool messageExists) AppendToStreamExpectedVersionNoStream(
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

            command.CommandText = @"SELECT (SELECT COUNT(*) > 0 
                FROM messages 
                WHERE messages.stream_id_internal = @streamIdInternal
                    AND messages.message_id = @messageId
                    AND messages.stream_version = 0) > 0;";
            command.Parameters.Clear();
            command.Parameters.AddWithValue("@streamIdInternal", _stream_id_internal);
            command.Parameters.AddWithValue("@messageId", message.MessageId);

            messageExists = Convert.ToBoolean(command.ExecuteScalar());

            if(!messageExists)
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

            return (nextExpectedVersion: 0, new AppendResult(currentVersion, currentPosition), messageExists);
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
            long _stream_id_internal = ResolveInternalStream(command, streamId, cancellationToken);

            command.CommandText = @"SELECT streams.version < @expectedVersion 
                                    FROM streams 
                                    WHERE streams.id_internal = @streamIdInternal;";
            command.Parameters.Clear();
            command.Parameters.AddWithValue("@streamIdInternal", _stream_id_internal);
            command.Parameters.AddWithValue("@expectedVersion", expectedVersion);

            if(Convert.ToBoolean(command.ExecuteScalar()))
            {
                throw new WrongExpectedVersionException(
                    ErrorMessages.AppendFailedWrongExpectedVersion(streamId.Id, expectedVersion),
                    streamId.Id,
                    expectedVersion);
            }
            
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
                                        VALUES(@streamIdInternal, @expectedVersion + 1, @messageId, @createdUtc, @type, @jsonData, @jsonMetadata);
                                        
                                        UPDATE streams SET [version] = @expectedVersion + 1, [position] = last_insert_rowid()
                                        WHERE streams.id_internal = @streamIdInternal;";
                
                command.Parameters.Clear();
                command.Parameters.AddWithValue("@streamIdInternal", _stream_id_internal);
                command.Parameters.AddWithValue("@expectedVersion", expectedVersion);
                command.Parameters.AddWithValue("@messageId", message.MessageId);
                command.Parameters.AddWithValue("@createdUtc", GetUtcNow());
                command.Parameters.AddWithValue("@type", message.Type);
                command.Parameters.AddWithValue("@jsonData", message.JsonData);
                command.Parameters.AddWithValue("@jsonMetadata", message.JsonMetadata);

                command.ExecuteNonQuery();

                command.CommandText = @"SELECT streams.position 
                                        FROM streams 
                                        WHERE streams.id_internal = @streamIdInternal";
                command.Parameters.Clear();
                command.Parameters.AddWithValue("@streamIdInternal", _stream_id_internal);

                currentVersion = expectedVersion + 1;
                currentPosition = Convert.ToInt64(command.ExecuteScalar());
            }
            else
            {
                command.CommandText = @"SELECT streams.version, streams.position
                                    FROM streams WHERE streams.id_internal = @streamIdInternal;";
                command.Parameters.Clear();
                command.Parameters.AddWithValue("@streamIdInternal", _stream_id_internal);

                using(var reader = command.ExecuteReader())
                {
                    currentVersion = reader.GetInt32(0);
                    currentPosition = reader.GetInt64(1);
                }
            }

            return (nextExpectedVersion: expectedVersion + 1, new AppendResult(currentVersion, currentPosition), messageExists);
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
                    command.CommandText = "SELECT streams.version > 0 FROM streams WHERE streams.id = @streamId";
                    command.Parameters.Clear();
                    command.Parameters.AddWithValue("@streamId", streamIdInfo.SQLiteStreamId.Id);

                    if(Convert.ToBoolean(command.ExecuteScalar()))
                    {
                        throw new WrongExpectedVersionException(
                            ErrorMessages.AppendFailedWrongExpectedVersion(streamIdInfo.SQLiteStreamId.Id, expectedVersion),
                            streamIdInfo.SQLiteStreamId.Id,
                            expectedVersion);
                    }
                }
                else if(expectedVersion == ExpectedVersion.NoStream)
                {
                    command.CommandText = "SELECT COUNT(*) > 0 FROM streams WHERE streams.id = @streamId";
                    command.Parameters.Clear();
                    command.Parameters.AddWithValue("@streamId", streamIdInfo.SQLiteStreamId.Id);

                    if(Convert.ToBoolean(command.ExecuteScalar()))
                    {
                        throw new WrongExpectedVersionException(
                            ErrorMessages.AppendFailedWrongExpectedVersion(streamIdInfo.SQLiteStreamId.Id, expectedVersion),
                            streamIdInfo.SQLiteStreamId.Id,
                            expectedVersion);
                    }
                }
                
                command.CommandText = "SELECT COUNT(*) = 0 FROM streams WHERE streams.id = @streamId";
                command.Parameters.Clear();
                command.Parameters.AddWithValue("@streamId", streamIdInfo.SQLiteStreamId.Id);

                if(Convert.ToBoolean(command.ExecuteScalar()))
                {
                    command.CommandText = @"INSERT INTO streams (id, id_original, max_age, max_count)
                                            VALUES(@streamId, @streamIdOriginal, @maxAge, @maxCount);
                                            SELECT last_insert_rowid();";
                    command.Parameters.Clear();
                    command.Parameters.AddWithValue("@streamId", streamIdInfo.SQLiteStreamId.Id);
                    command.Parameters.AddWithValue("@streamIdOriginal", streamIdInfo.SQLiteStreamId.IdOriginal);
                    command.Parameters.AddWithValue("@maxAge", DBNull.Value);
                    command.Parameters.AddWithValue("@maxCount", DBNull.Value);

                    command.ExecuteScalar();
                }

                command.CommandText = "SELECT streams.version, streams.position FROM streams WHERE streams.id = @streamId";
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

        ///  anything below this is possible to be destroyed/removed.
        private void CheckStreamMaxCount(string streamId, int? maxCount, CancellationToken cancellationToken)
        {
            if (maxCount.HasValue)
            {
                var count = GetStreamMessageCount(streamId, cancellationToken);
                if (count > maxCount.Value)
                {
                    int toPurge = count - maxCount.Value;

                    var streamMessagesPage = ReadStreamForwardsInternal(streamId, StreamVersion.Start,
                        toPurge, false, null, cancellationToken).GetAwaiter().GetResult();

                    if (streamMessagesPage.Status == PageReadStatus.Success)
                    {
                        foreach (var message in streamMessagesPage.Messages)
                        {
                            DeleteEventInternal(streamId, message.MessageId, cancellationToken);
                        }
                    }
                }
            }
        }

        private int GetStreamMessageCount(
            string streamId,
            CancellationToken cancellationToken = default)
        {
            GuardAgainstDisposed();

            using(var connection = OpenConnection())
            {
                using(var command = new SqliteCommand(_scripts.GetStreamMessageCount, connection))
                {
                    var streamIdInfo = new StreamIdInfo(streamId);
                    command.Parameters.AddWithValue("@streamId", streamId);

                    return (int) command.ExecuteScalar();
                }
            }
        }

        private (int? maxAge, int? maxCount) GetStreamMetadata(string metadataStreamId, 
            SqliteCommand command)
        {
            var maxAge = default(int?);
            var maxCount = default(int?);
            
            command.CommandText = @"SELECT id_internal FROM streams WHERE streams.id = @metadataStreamId";
            command.Parameters.Clear();
            command.Parameters.AddWithValue("@metadataStreamId", metadataStreamId);
            var streamIdInternal = command.ExecuteScalar();

            if(streamIdInternal != DBNull.Value)
            {
                command.CommandText =
                    @"SELECT messages.json_data 
FROM messages 
WHERE messages.stream_id_internal = @streamIdInternal 
ORDER BY messages.stream_version 
DESC LIMIT 1";
                command.Parameters.Clear();
                command.Parameters.AddWithValue("@streamIdInternal", streamIdInternal);

                var jsonField = command.ExecuteScalar();

                if(jsonField != DBNull.Value)
                {
                    var data = jsonField.ToString();

                    var startIndex = data.IndexOf("\"MaxAge\":") + 9;
                    var length = data.IndexOf(",", startIndex);
                    var metadataValue = data.Substring(startIndex, length);
                    maxAge = (metadataValue.Equals("null") || string.IsNullOrEmpty(metadataValue))
                        ? default(int?)
                        : Convert.ToInt32(metadataValue);
                    
                    startIndex = data.IndexOf("\"MaxAge\":") + 11;
                    length = data.IndexOf(",", startIndex);
                    metadataValue = data.Substring(startIndex, length);
                    maxCount = (metadataValue.Equals("null") || string.IsNullOrEmpty(metadataValue))
                        ? default(int?)
                        : Convert.ToInt32(metadataValue);
                }
            }

            return (maxAge, maxCount);
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