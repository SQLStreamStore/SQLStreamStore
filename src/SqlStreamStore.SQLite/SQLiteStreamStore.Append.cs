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
            using (var connection = await OpenConnection(cancellationToken))
            using(var transaction = connection.BeginTransaction())
            {
                var throwIfAdditionalMessages = false;

                for(var i = 0; i < messages.Length; i++)
                {
                    bool messageExists;
                    (nextExpectedVersion, appendResult, messageExists) = await AppendMessageToStream(
                        connection,
                        streamId.SQLiteStreamId,
                        nextExpectedVersion,
                        messages[i],
                        transaction,
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
            SQLiteConnection connection,
            SQLiteStreamId streamId, 
            int expectedVersion, 
            NewStreamMessage message, 
            SQLiteTransaction transaction, 
            CancellationToken cancellationToken)
        {
            switch(expectedVersion)
            {
                case ExpectedVersion.Any:
                    return await AppendToStreamExpectedVersionAny(connection, streamId, message, transaction, cancellationToken);
                case ExpectedVersion.NoStream:
                    return await AppendToStreamExpectedVersionNoStream(connection, streamId, message, transaction, cancellationToken);
                case ExpectedVersion.EmptyStream:
                    return await AppendToStreamExpectedVersionEmptyStream(connection, streamId, message, transaction, cancellationToken);
                default:
                    return await AppendToStreamExpectedVersion(connection, streamId, expectedVersion, message, transaction, cancellationToken);
            }
        }

        private async Task<(int nextExpectedVersion, AppendResult appendResult, bool messageExists)> AppendToStreamExpectedVersionAny(
            SQLiteConnection connection,
            SQLiteStreamId streamId, 
            NewStreamMessage message, 
            SQLiteTransaction transaction, 
            CancellationToken cancellationToken)
        {
            using(var command = connection.CreateCommand())
            {
                // get internal id
                long _stream_id_internal;
                command.CommandText = "SELECT id_internal FROM streams WHERE id = @streamId";
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
                        streamVersion = reader.GetInt32(1);
                    }
                }

                if(!messageExists)
                {
                    command.CommandText = @"INSERT INTO messages(stream_id_internal, stream_version, message_id, created_utc, type, json_data, json_metadata)
                    SELECT @streamIdInternal, @streamVersion, @messageId, @createdUtc, @type, @jsonData, @jsonMetadata)
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
        }

        private async Task<(int nextExpectedVersion, AppendResult appendResult, bool messageExists)> AppendToStreamExpectedVersionNoStream(
            SQLiteConnection connection,
            SQLiteStreamId streamId, 
            NewStreamMessage message, 
            SQLiteTransaction transaction, 
            CancellationToken cancellationToken)
        {
            throw new NotImplementedException();
        }

        private async Task<(int nextExpectedVersion, AppendResult appendResult, bool messageExists)> AppendToStreamExpectedVersionEmptyStream(
            SQLiteConnection connection,
            SQLiteStreamId streamId, 
            NewStreamMessage message, 
            SQLiteTransaction transaction, 
            CancellationToken cancellationToken)
        {
            throw new NotImplementedException();
        }

        private async Task<(int nextExpectedVersion, AppendResult appendResult, bool messageExists)> AppendToStreamExpectedVersion(
            SQLiteConnection connection,
            SQLiteStreamId streamId, 
            int expectedVersion, 
            NewStreamMessage message, 
            SQLiteTransaction transaction, 
            CancellationToken cancellationToken)
        {
            throw new NotImplementedException();
        }

        private async Task<AppendResult> CreateEmptyStream(StreamIdInfo streamIdInfo, int expectedVersion, CancellationToken cancellationToken)
        {
            throw new NotImplementedException();
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