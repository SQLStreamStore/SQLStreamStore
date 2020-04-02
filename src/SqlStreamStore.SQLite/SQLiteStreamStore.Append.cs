namespace SqlStreamStore
{
    using System;
    using System.Data.SQLite;
    using System.Threading;
    using System.Threading.Tasks;
    using SqlStreamStore.Infrastructure;
    using SqlStreamStore.Streams;

    partial class SQLiteStreamStore
    {
        protected override async Task<AppendResult> AppendToStreamInternal(
            string streamId, 
            int expectedVersion, 
            NewStreamMessage[] messages, 
            CancellationToken cancellationToken)
        {
            var streamIdInfo = new StreamIdInfo(streamId);

            try
            {
                return messages.Length == 0
                    ? await CreateEmptyStream(streamIdInfo, expectedVersion, cancellationToken)
                    : await AppendMessagesToStream(streamIdInfo, expectedVersion, messages, cancellationToken);
            }
            catch(Exception e)
            {
                Console.WriteLine(e);
                throw;
            }
        }

        private async Task<AppendResult> AppendMessagesToStream(
            StreamIdInfo streamId, 
            int expectedVersion, 
            NewStreamMessage[] messages, 
            CancellationToken cancellationToken)
        {
            var appendResult = new AppendResult(StreamVersion.End, Position.End);
            var nextExpectedVersion = expectedVersion;
            
            using(var connection = await OpenConnection(cancellationToken))
            using(var transaction = connection.BeginTransaction())
            using(var command = connection.CreateCommand())
            {
                command.Transaction = transaction;
                
                var throwIfAdditionalMessages = false;

                for(var i = 0; i < messages.Length; i++)
                {
                    bool messageExists;
                    (nextExpectedVersion, appendResult, messageExists) = await AppendMessageToStream(
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

            await TryScavenge(streamId, cancellationToken);

            return appendResult;
        }

        private async Task<(int nextExpectedVersion, AppendResult appendResult, bool messageExists)> AppendMessageToStream(
            SQLiteCommand command,
            SQLiteStreamId streamId, 
            int expectedVersion, 
            NewStreamMessage message, 
            CancellationToken cancellationToken)
        {
            switch(expectedVersion)
            {
                case ExpectedVersion.Any:
                    return await AppendToStreamExpectedVersionAny(command, streamId, message, cancellationToken);
                case ExpectedVersion.NoStream:
                    return await AppendToStreamExpectedVersionNoStream(command, streamId, message, cancellationToken);
                case ExpectedVersion.EmptyStream:
                    return await AppendToStreamExpectedVersionEmptyStream(command, streamId, message, cancellationToken);
                default:
                    return await AppendToStreamExpectedVersion(command, streamId, expectedVersion, message, cancellationToken);
            }
        }

        private async Task<(int nextExpectedVersion, AppendResult appendResult, bool messageExists)> AppendToStreamExpectedVersionAny(
            SQLiteCommand command,
            SQLiteStreamId streamId, 
            NewStreamMessage message, 
            CancellationToken cancellationToken)
        {
                // get internal id
                long _stream_id_internal = await ResolveInternalStream(command, streamId, cancellationToken);

                command.CommandText = @"SELECT COUNT(*) > 0, stream_version
FROM messages
WHERE messages.stream_id_internal = @streamIdInternal
    AND messages.message_id = @messageId";
                command.Parameters.Clear();
                command.Parameters.Add(new SQLiteParameter("@streamIdInternal", _stream_id_internal));
                command.Parameters.Add(new SQLiteParameter("@messageId", message.MessageId));

                bool messageExists = false;
                int streamVersion = -1;
                var streamPosition = -1;
                
                using(var reader = await command.ExecuteReaderAsync(cancellationToken))
                {
                    if(await reader.ReadAsync(cancellationToken))
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
                    SET streams.version = @streamVersion,
                        streams.position = last_insert_rowid() // current position
                    WHERE streams.id_internal = @streamIdInternal;";
                    command.Parameters.Clear();
                    command.Parameters.Add(new SQLiteParameter("@streamIdInternal", _stream_id_internal));
                    command.Parameters.Add(new SQLiteParameter("@streamVersion", streamVersion + 1));
                    await command.ExecuteNonQueryAsync(cancellationToken);
                }
                else
                {
                    command.CommandText = @"SELECT streams.version, streams.position 
FROM streams 
WHERE streams.id_internal = @streamIdInternal;";
                    command.Parameters.Clear();
                    command.Parameters.Add(new SQLiteParameter("@streamIdInternal", _stream_id_internal));

                    using(var reader = await command.ExecuteReaderAsync(cancellationToken))
                    {
                        await reader.ReadAsync(cancellationToken);
                        streamVersion = reader.GetInt32(0);
                        streamPosition = reader.GetInt32(1);
                    }
                }
                
                return (streamVersion, 
                    new AppendResult(streamVersion + 1, streamPosition), 
                    messageExists);

        }

        private async Task<(int nextExpectedVersion, AppendResult appendResult, bool messageExists)> AppendToStreamExpectedVersionNoStream(
            SQLiteCommand command,
            SQLiteStreamId streamId, 
            NewStreamMessage message, 
            CancellationToken cancellationToken)
        {
            int currentVersion = 0;
            long currentPosition = 0;
            bool messageExists = false;

            // get internal id
            long _stream_id_internal = await ResolveInternalStream(command, streamId, cancellationToken);

            command.CommandText = @"SELECT COUNT(*) > 0 
                FROM messages 
                WHERE messages.stream_id_internal = @streamIdInternal
                    AND messages.message_id = @messageId
                    AND messages.stream_version = 0;";
            command.Parameters.Clear();
            command.Parameters.AddWithValue("@streamIdInternal", _stream_id_internal);
            command.Parameters.AddWithValue("@messageId", message.MessageId);

            messageExists = Convert.ToBoolean(await command.ExecuteScalarAsync(cancellationToken));

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

                await command.ExecuteNonQueryAsync(cancellationToken);
            }

            command.CommandText = "SELECT streams.version, streams.position FROM streams WHERE streams.id_internal = @streamIdInternal";
            command.Parameters.Clear();
            command.Parameters.AddWithValue("@streamIdInternal", _stream_id_internal);

            using(var reader = await command.ExecuteReaderAsync(cancellationToken))
            {
                await reader.ReadAsync(cancellationToken);
                currentVersion = reader.GetInt32(0);
                currentPosition = reader.GetInt64(1);
            }

            return (nextExpectedVersion: 0, new AppendResult(currentVersion, currentPosition), messageExists);
        }

        private async Task<(int nextExpectedVersion, AppendResult appendResult, bool messageExists)> AppendToStreamExpectedVersionEmptyStream(
            SQLiteCommand command,
            SQLiteStreamId streamId, 
            NewStreamMessage message,  
            CancellationToken cancellationToken)
        {
            int currentVersion = 0;
            long currentPosition = 0;
            bool messageExists = false;

            // get internal id
            long _stream_id_internal = await ResolveInternalStream(command, streamId, cancellationToken);

            command.CommandText = @"SELECT COUNT(*) > 0 
                FROM messages 
                WHERE messages.stream_id_internal = @streamIdInternal
                    AND messages.message_id = @messageId
                    AND messages.stream_version = 0;";
            command.Parameters.Clear();
            command.Parameters.AddWithValue("@streamIdInternal", _stream_id_internal);
            command.Parameters.AddWithValue("@messageId", message.MessageId);

            messageExists = Convert.ToBoolean(await command.ExecuteScalarAsync(cancellationToken));

            if(!messageExists)
            {
                command.CommandText = @"INSERT INTO messages (stream_id_internal, stream_version, message_id, created_utc, type, json_data, json_metadata)
                                        VALUES(@streamIdInternal, 0, @messageId, @createdUtc, @type, @jsonData, @jsonMetadata);
                                        
                                        UPDATE streams SET streams.version = 0, streams.position = last_insert_rowid()
                                        WHERE streams.id_internal = @streamIdInternal;";
                
                command.Parameters.Clear();
                command.Parameters.AddWithValue("@streamIdInternal", _stream_id_internal);
                command.Parameters.AddWithValue("@messageId", message.MessageId);
                command.Parameters.AddWithValue("@createdUtc", GetUtcNow());
                command.Parameters.AddWithValue("@type", message.Type);
                command.Parameters.AddWithValue("@jsonData", message.JsonData);
                command.Parameters.AddWithValue("@jsonMetadata", message.JsonMetadata);

                await command.ExecuteNonQueryAsync(cancellationToken);
            }

            command.CommandText = @"SELECT 0, messages.position 
                                    FROM messages 
                                    WHERE messages.message_id = @messageId 
                                        AND messages.stream_id_internal = @streamIdInternal;";
            command.Parameters.Clear();
            command.Parameters.AddWithValue("@messageId", message.MessageId);
            command.Parameters.AddWithValue("@streamIdInternal", _stream_id_internal);

            using(var reader = await command.ExecuteReaderAsync(cancellationToken))
            {
                await reader.ReadAsync(cancellationToken);
                currentVersion = reader.GetInt32(0);
                currentPosition = reader.GetInt64(1);
            }

            return (nextExpectedVersion: 0, new AppendResult(currentVersion, currentPosition), messageExists);
        }

        private async Task<(int nextExpectedVersion, AppendResult appendResult, bool messageExists)> AppendToStreamExpectedVersion(
            SQLiteCommand command,
            SQLiteStreamId streamId, 
            int expectedVersion, 
            NewStreamMessage message, 
            CancellationToken cancellationToken)
        {
            int currentVersion = 0;
            long currentPosition = 0;
            bool messageExists = false;
            
            // get internal id
            long _stream_id_internal = await ResolveInternalStream(command, streamId, cancellationToken);

            command.CommandText = @"SELECT streams.version < @expectedVersion 
                                    FROM streams 
                                    WHERE streams.id_internal = @streamIdInternal;";
            command.Parameters.Clear();
            command.Parameters.AddWithValue("@streamIdInternal", _stream_id_internal);
            command.Parameters.AddWithValue("@expectedVersion", expectedVersion);

            if((bool) await command.ExecuteScalarAsync(cancellationToken))
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

            messageExists = Convert.ToBoolean(await command.ExecuteScalarAsync(cancellationToken));

            if(!messageExists)
            {
                command.CommandText = @"INSERT INTO messages (stream_id_internal, stream_version, message_id, created_utc, type, json_data, json_metadata)
                                        VALUES(@streamIdInternal, @expectedVersion + 1, @messageId, @createdUtc, @type, @jsonData, @jsonMetadata);
                                        
                                        UPDATE streams SET streams.version = @expectedVersion + 1, streams.position = last_insert_rowid()
                                        WHERE streams.id_internal = @streamIdInternal;";
                
                command.Parameters.Clear();
                command.Parameters.AddWithValue("@streamIdInternal", _stream_id_internal);
                command.Parameters.AddWithValue("@expectedVersion", expectedVersion);
                command.Parameters.AddWithValue("@messageId", message.MessageId);
                command.Parameters.AddWithValue("@createdUtc", GetUtcNow());
                command.Parameters.AddWithValue("@type", message.Type);
                command.Parameters.AddWithValue("@jsonData", message.JsonData);
                command.Parameters.AddWithValue("@jsonMetadata", message.JsonMetadata);

                await command.ExecuteNonQueryAsync(cancellationToken);

                command.CommandText = @"SELECT streams.position 
                                        FROM streams 
                                        WHERE streams.id_internal = @streamIdInternal";
                command.Parameters.Clear();
                command.Parameters.AddWithValue("@streamIdInternal", _stream_id_internal);

                currentVersion = expectedVersion + 1;
                currentPosition = Convert.ToInt64(await command.ExecuteScalarAsync(cancellationToken));
            }
            else
            {
                command.CommandText = @"SELECT streams.version, streams.position
                                    FROM streams WHERE streams.id_internal = @streamIdInternal;";
                command.Parameters.Clear();
                command.Parameters.AddWithValue("@streamIdInternal", _stream_id_internal);

                using(var reader = await command.ExecuteReaderAsync(cancellationToken))
                {
                    currentVersion = reader.GetInt32(0);
                    currentPosition = reader.GetInt64(1);
                }
            }

            return (nextExpectedVersion: expectedVersion + 1, new AppendResult(currentVersion, currentPosition), messageExists);
        }

        private async Task<AppendResult> CreateEmptyStream(
            StreamIdInfo streamIdInfo,
            int expectedVersion,
            CancellationToken cancellationToken)
        {
            var appendResult = new AppendResult(StreamVersion.End, Position.End);
            
            using(var connection = await OpenConnection(cancellationToken))
            using(var transaction = connection.BeginTransaction())
            using (var command = new SQLiteCommand("", connection, transaction))
            {
                if(expectedVersion == ExpectedVersion.EmptyStream)
                {
                    command.CommandText = "SELECT streams.version > 0 FROM streams WHERE streams.id = @streamId";
                    command.Parameters.Clear();
                    command.Parameters.Add(new SQLiteParameter("@streamId", streamIdInfo.SQLiteStreamId.Id));

                    if(Convert.ToBoolean(await command.ExecuteScalarAsync(cancellationToken)))
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
                    command.Parameters.Add(new SQLiteParameter("@streamId", streamIdInfo.SQLiteStreamId.Id));

                    if(Convert.ToBoolean(await command.ExecuteScalarAsync(cancellationToken)))
                    {
                        throw new WrongExpectedVersionException(
                            ErrorMessages.AppendFailedWrongExpectedVersion(streamIdInfo.SQLiteStreamId.Id, expectedVersion),
                            streamIdInfo.SQLiteStreamId.Id,
                            expectedVersion);
                    }
                }
                
                command.CommandText = "SELECT COUNT(*) = 0 FROM streams WHERE streams.id = @streamId";
                command.Parameters.Clear();
                command.Parameters.Add(new SQLiteParameter("@streamId", streamIdInfo.SQLiteStreamId.Id));

                if(Convert.ToBoolean(await command.ExecuteScalarAsync(cancellationToken)))
                {
                    command.CommandText = @"INSERT INTO streams (id, id_original, max_age, max_count)
                                            VALUES(@streamId, @streamIdOriginal, @maxAge, @maxCount);
                                            SELECT last_insert_rowid();";
                    command.Parameters.Clear();
                    command.Parameters.Add(new SQLiteParameter("@streamId", streamIdInfo.SQLiteStreamId.Id));
                    command.Parameters.Add(new SQLiteParameter("@streamOriginalId", streamIdInfo.SQLiteStreamId.IdOriginal));
                    command.Parameters.Add(new SQLiteParameter("@maxAge", DBNull.Value));
                    command.Parameters.Add(new SQLiteParameter("@maxCount", DBNull.Value));

                    await command.ExecuteScalarAsync(cancellationToken);
                }

                command.CommandText = "SELECT streams.version, streams.position FROM streams WHERE streams.id = @streamId";
                command.Parameters.Clear();
                command.Parameters.Add(new SQLiteParameter("@streamId", streamIdInfo.SQLiteStreamId.Id));

                using(var reader = await command.ExecuteReaderAsync(cancellationToken))
                {
                    await reader.ReadAsync(cancellationToken);
                    appendResult = new AppendResult(reader.GetInt32(0), reader.GetInt64(1));
                }
                
                transaction.Commit();
            }

            return appendResult;
        }

        ///  anything below this is possible to be destroyed/removed.
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

        private async Task<int> GetStreamMessageCount(
            string streamId,
            CancellationToken cancellationToken = default)
        {
            GuardAgainstDisposed();

            using(var connection = await OpenConnection(cancellationToken))
            {
                using(var command = new SQLiteCommand(_scripts.GetStreamMessageCount, connection))
                {
                    var streamIdInfo = new StreamIdInfo(streamId);
                    command.Parameters.Add(new SQLiteParameter("@streamId", streamId));

                    var result = await command
                        .ExecuteScalarAsync(cancellationToken)
                        .NotOnCapturedContext();

                    return (int) result;
                }
            }
        }

        private async Task<(int? maxAge, int? maxCount)> GetStreamMetadata(string metadataStreamId, 
            SQLiteCommand command)
        {
            var maxAge = default(int?);
            var maxCount = default(int?);
            
            command.CommandText = @"SELECT id_internal FROM streams WHERE streams.id = @metadataStreamId";
            command.Parameters.Clear();
            command.Parameters.Add(new SQLiteParameter("@metadataStreamId", metadataStreamId));
            var streamIdInternal = await command.ExecuteScalarAsync();

            if(streamIdInternal != DBNull.Value)
            {
                command.CommandText =
                    @"SELECT messages.json_data 
FROM messages 
WHERE messages.stream_id_internal = @streamIdInternal 
ORDER BY messages.stream_version 
DESC LIMIT 1";
                command.Parameters.Clear();
                command.Parameters.Add(new SQLiteParameter("@streamIdInternal", streamIdInternal));

                var jsonField = await command.ExecuteScalarAsync();

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

        private async Task<long> ResolveInternalStream(SQLiteCommand command, SQLiteStreamId streamId, CancellationToken cancellationToken)
        {
            // get internal id
            long _stream_id_internal;
            command.CommandText = "SELECT id_internal FROM streams WHERE id = @streamId";
            command.Parameters.Clear();
            command.Parameters.Add(new SQLiteParameter("@streamId", streamId.Id));
            var result = await command.ExecuteScalarAsync(cancellationToken);
            if(result == DBNull.Value)
            {
                command.CommandText = @"INSERT INTO streams (id, id_original, max_age, max_count)
                                            VALUES(@streamId, @streamIdOriginal, @maxAge, @maxCount);
                                            SELECT last_insert_rowid();";
                command.Parameters.Clear();
                command.Parameters.Add(new SQLiteParameter("@streamId", streamId.Id));
                command.Parameters.Add(new SQLiteParameter("@streamOriginalId", streamId.IdOriginal));
                command.Parameters.Add(new SQLiteParameter("@maxAge", DBNull.Value));
                command.Parameters.Add(new SQLiteParameter("@maxCount", DBNull.Value));
                _stream_id_internal = Convert.ToInt64(await command.ExecuteScalarAsync(cancellationToken));
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