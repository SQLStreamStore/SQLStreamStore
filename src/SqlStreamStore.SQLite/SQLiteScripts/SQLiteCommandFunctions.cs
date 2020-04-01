namespace SqlStreamStore
{
    using System;
    using System.Data;
    using System.Data.SQLite;
    using System.Linq;
    using System.Threading;
    using System.Threading.Tasks;
    using SqlStreamStore.Infrastructure;
    using SqlStreamStore.Streams;
    using StreamStoreStore.Json;

    internal static class SQLiteCommandFunctions
    {
        public static async Task<SQLiteAppendResult> AppendToStreamExpectedVersionAny(
            this SQLiteConnection connection, 
            SQLiteTransaction transaction, 
            SQLiteStreamId streamId, 
            NewStreamMessage[] messages,
            GetUtcNow getUtcNow,
            CancellationToken cancellationToken)
        {
            int? latestStreamVersion = default(int?);
            long? latestStreamPosition = default(long?);
            int streamIdInternal = -1;

            using(var command = new SQLiteCommand(string.Empty, connection, transaction))
            {
                command.CommandText = @"IF NOT EXISTS (SELECT * FROM streams WHERE streams.id = @streamId)
                BEGIN
                INSERT INTO streams (id, id_original)
                VALUES (@streamId, @streamIdOriginal)
                END";
                command.Parameters.Add(new SQLiteParameter("@streamId", streamId.Id));
                command.Parameters.Add(new SQLiteParameter("@streamIdOriginal", streamId.IdOriginal));
                await command.ExecuteNonQueryAsync();

                command.CommandText = "SELECT id_internal, version, position FROM streams WHERE streams.id = @streamId";
                command.Parameters.Clear();
                command.Parameters.Add(new SQLiteParameter("@streamId", streamId.Id));

                using (var reader = await command.ExecuteReaderAsync())
                {
                    if (await reader.ReadAsync())
                    {
                        streamIdInternal = reader.GetInt32(0);
                        latestStreamVersion = reader.GetInt32(1);
                        latestStreamPosition = reader.GetInt64(2);
                    }
                }

                if (messages.Any())
                {
                    command.CommandText = @"INSERT INTO messages (stream_id_internal, stream_version, message_id, created_utc, type, json_data, json_metadata)
                    VALUES(@streamIdInternal, (StreamVersion + @latestStreamVersion + 1), @messageId, @createdUtc, @type, @jsonData, @jsonMetadata);
                    SELECT last_insert_rowid() position, MAX(stream_version)
                    FROM messages
                    WHERE stream_id_internal = @streamIdInternal";
                    command.Parameters.Clear();
                    command.Parameters.Add(new SQLiteParameter("@streamIdInternal", DbType.Int32));
                    command.Parameters.Add(new SQLiteParameter("@latestStreamVersion", DbType.Int32));
                    command.Parameters.Add(new SQLiteParameter("@messageId", DbType.StringFixedLength, 36));
                    command.Parameters.Add(new SQLiteParameter("@createdUtc", DbType.DateTime));
                    command.Parameters.Add(new SQLiteParameter("@type", DbType.StringFixedLength, 128));
                    command.Parameters.Add(new SQLiteParameter("@jsonData", DbType.String));
                    command.Parameters.Add(new SQLiteParameter("@jsonMetadata", DbType.String));

                    var created = getUtcNow.Invoke();
                    foreach (var message in messages)
                    {
                        command.Parameters["@streamIdInternal"].Value = streamIdInternal;
                        command.Parameters["@latestStreamVersion"].Value = latestStreamVersion;
                        command.Parameters["@messageId"].Value = message.MessageId;
                        command.Parameters["@createdUtc"].Value = created;
                        command.Parameters["@type"].Value = message.Type;
                        command.Parameters["@jsonData"].Value = message.JsonData;
                        command.Parameters["@jsonMetadata"].Value = message.JsonMetadata;

                        using (var reader = await command.ExecuteReaderAsync())
                        {
                            await reader.ReadAsync();
                            latestStreamPosition = reader.GetInt32(0);
                            latestStreamVersion = reader.GetInt32(1);
                        }
                    }

                    command.CommandText = @"UPDATE streams
                    SET streams.version = @latestStreamVersion
                        streams.position = @latestStreamPosition
                    WHERE streams.stream_id_internal = @streamIdInternal;";
                    command.Parameters.Clear();
                    command.Parameters.Add(new SQLiteParameter("@streamIdInternal", streamIdInternal));
                    command.Parameters.Add(new SQLiteParameter("@latestStreamVersion", latestStreamVersion));
                    command.Parameters.Add(new SQLiteParameter("@latestStreamPosition", latestStreamPosition));
                }
                else
                {
                    latestStreamPosition = latestStreamPosition ?? -1;
                    latestStreamVersion = latestStreamVersion ?? -1;
                }

                command.CommandText = @"SELECT messages.json_data
                FROM messages
                WHERE messages.position = (
                    SELECT streams.position
                    FROM streams
                    WHERE streams.id = '$$' + @streamId
                );";
                command.Parameters.Clear();
                command.Parameters.Add(new SQLiteParameter("@streamId", streamId.Id));

                var maxCount = default(int?);
                using (var reader = await command.ExecuteReaderAsync())
                {
                    var jsonData = reader.GetString(0);
                    var metadataMessage = SimpleJson.DeserializeObject<MetadataMessage>(jsonData);
                    maxCount = metadataMessage.MaxCount;
                }

                return new SQLiteAppendResult(maxCount, latestStreamVersion.Value, latestStreamPosition.Value);
            }
        }

        public static async Task<SQLiteAppendResult> AppendToStreamExpectedVersionNoStream(
            this SQLiteConnection connection, 
            SQLiteTransaction transaction, 
            SQLiteStreamId streamId, 
            NewStreamMessage[] messages,
            GetUtcNow getUtcNow,
            CancellationToken cancellationToken)
        {
            int? latestStreamVersion = default(int?);
            long? latestStreamPosition = default(long?);
            int streamIdInternal = -1;

            using (var command = connection.CreateCommand())
            {
                command.Transaction = transaction;
                command.CommandText = @"IF NOT EXISTS (SELECT * FROM streams WHERE streams.id = @streamId)
                BEGIN
                INSERT INTO streams (id, id_original)
                VALUES (@streamId, @streamIdOriginal)
                END";
                command.Parameters.Add(new SQLiteParameter("@streamId", streamId.Id));
                command.Parameters.Add(new SQLiteParameter("@streamIdOriginal", streamId.IdOriginal));
                await command.ExecuteNonQueryAsync();

                if (messages.Any())
                {
                    command.CommandText = @"INSERT INTO messages (stream_id_internal, stream_version, message_id, created_utc, type, json_data, json_metadata)
                    VALUES(@streamIdInternal, (StreamVersion + @latestStreamVersion + 1), @messageId, @createdUtc, @type, @jsonData, @jsonMetadata);
                    SELECT last_insert_rowid() position, MAX(stream_version)
                    FROM messages
                    WHERE stream_id_internal = @streamIdInternal";
                    command.Parameters.Clear();
                    command.Parameters.Add(new SQLiteParameter("@streamIdInternal", DbType.Int32));
                    command.Parameters.Add(new SQLiteParameter("@latestStreamVersion", DbType.Int32));
                    command.Parameters.Add(new SQLiteParameter("@messageId", DbType.StringFixedLength, 36));
                    command.Parameters.Add(new SQLiteParameter("@createdUtc", DbType.DateTime));
                    command.Parameters.Add(new SQLiteParameter("@type", DbType.StringFixedLength, 128));
                    command.Parameters.Add(new SQLiteParameter("@jsonData", DbType.String));
                    command.Parameters.Add(new SQLiteParameter("@jsonMetadata", DbType.String));

                    var created = getUtcNow.Invoke();
                    foreach (var message in messages)
                    {
                        command.Parameters["@streamIdInternal"].Value = streamIdInternal;
                        command.Parameters["@latestStreamVersion"].Value = latestStreamVersion;
                        command.Parameters["@messageId"].Value = message.MessageId;
                        command.Parameters["@createdUtc"].Value = created;
                        command.Parameters["@type"].Value = message.Type;
                        command.Parameters["@jsonData"].Value = message.JsonData;
                        command.Parameters["@jsonMetadata"].Value = message.JsonMetadata;

                        using (var reader = await command.ExecuteReaderAsync())
                        {
                            await reader.ReadAsync();
                            latestStreamPosition = reader.GetInt32(0);
                            latestStreamVersion = reader.GetInt32(1);
                        }
                    }

                    command.CommandText = @"UPDATE streams
                    SET streams.version = @latestStreamVersion
                        streams.position = @latestStreamPosition
                    WHERE streams.stream_id_internal = @streamIdInternal;";
                    command.Parameters.Clear();
                    command.Parameters.Add(new SQLiteParameter("@streamIdInternal", streamIdInternal));
                    command.Parameters.Add(new SQLiteParameter("@latestStreamVersion", latestStreamVersion));
                    command.Parameters.Add(new SQLiteParameter("@latestStreamPosition", latestStreamPosition));
                }
                else
                {
                    latestStreamPosition = -1;
                    latestStreamVersion = -1;
                }

                command.CommandText = @"SELECT messages.json_data
                FROM messages
                WHERE messages.position = (
                    SELECT streams.position
                    FROM streams
                    WHERE streams.id = '$$' + @streamId
                );";
                command.Parameters.Clear();
                command.Parameters.Add(new SQLiteParameter("@streamId", streamId.Id));

                var maxCount = default(int?);
                using (var reader = await command.ExecuteReaderAsync())
                {
                    var jsonData = reader.GetString(0);
                    var metadataMessage = SimpleJson.DeserializeObject<MetadataMessage>(jsonData);
                    maxCount = metadataMessage.MaxCount;
                }

                return new SQLiteAppendResult(maxCount, latestStreamVersion.Value, latestStreamPosition.Value);
            }
       }

        public static async Task<SQLiteAppendResult> AppendToStreamExpectedVersion(
            this SQLiteConnection connection, 
            SQLiteTransaction transaction, 
            SQLiteStreamId streamId, 
            int expectedVersion,
            NewStreamMessage[] messages,
            GetUtcNow getUtcNow,
            CancellationToken cancellationToken)
        {
            var streamIdInternal = default(int?);
            var latestStreamVersion = default(int?);
            var latestStreamPosition = default(long?);


            using (var command = connection.CreateCommand())
            {
                command.Transaction = transaction;
                command.CommandText = @"SELECT stream_id_internal, version
                FROM streams
                WHERE streams.id = @streamId;";

                using (var reader = await command.ExecuteReaderAsync())
                {
                    if (await reader.ReadAsync())
                    {
                        streamIdInternal = reader.GetInt32(0);
                        latestStreamVersion = reader.GetInt32(1);
                    }
                }

                if (streamIdInternal == null || latestStreamVersion != expectedVersion)
                {
                    throw new SQLiteException("WrongExpectedVersion");
                }

                command.CommandText = @"INSERT INTO messages (stream_id_internal, stream_version, message_id, created_utc, type, json_data, json_metadata)
                VALUES(@streamIdInternal, (StreamVersion + @latestStreamVersion + 1), @messageId, @createdUtc, @type, @jsonData, @jsonMetadata);
                SELECT last_insert_rowid() position, MAX(stream_version)
                FROM messages
                WHERE stream_id_internal = @streamIdInternal";
                command.Parameters.Clear();
                command.Parameters.Add(new SQLiteParameter("@streamIdInternal", DbType.Int32));
                command.Parameters.Add(new SQLiteParameter("@latestStreamVersion", DbType.Int32));
                command.Parameters.Add(new SQLiteParameter("@messageId", DbType.StringFixedLength, 36));
                command.Parameters.Add(new SQLiteParameter("@createdUtc", DbType.DateTime));
                command.Parameters.Add(new SQLiteParameter("@type", DbType.StringFixedLength, 128));
                command.Parameters.Add(new SQLiteParameter("@jsonData", DbType.String));
                command.Parameters.Add(new SQLiteParameter("@jsonMetadata", DbType.String));

                var created = getUtcNow.Invoke();
                foreach (var message in messages)
                {
                    command.Parameters["@streamIdInternal"].Value = streamIdInternal;
                    command.Parameters["@latestStreamVersion"].Value = latestStreamVersion;
                    command.Parameters["@messageId"].Value = message.MessageId;
                    command.Parameters["@createdUtc"].Value = created;
                    command.Parameters["@type"].Value = message.Type;
                    command.Parameters["@jsonData"].Value = message.JsonData;
                    command.Parameters["@jsonMetadata"].Value = message.JsonMetadata;

                    using (var reader = await command.ExecuteReaderAsync())
                    {
                        await reader.ReadAsync();
                        latestStreamPosition = reader.GetInt32(0);
                        latestStreamVersion = reader.GetInt32(1);
                    }
                }

                command.CommandText = @"UPDATE streams
                SET streams.version = @latestStreamVersion
                    streams.position = @latestStreamPosition
                WHERE streams.stream_id_internal = @streamIdInternal;";
                command.Parameters.Clear();
                command.Parameters.Add(new SQLiteParameter("@streamIdInternal", streamIdInternal));
                command.Parameters.Add(new SQLiteParameter("@latestStreamVersion", latestStreamVersion));
                command.Parameters.Add(new SQLiteParameter("@latestStreamPosition", latestStreamPosition));
 
                command.CommandText = @"SELECT messages.json_data
                FROM messages
                WHERE messages.position = (
                    SELECT streams.position
                    FROM streams
                    WHERE streams.id = '$$' + @streamId
                );";
                command.Parameters.Clear();
                command.Parameters.Add(new SQLiteParameter("@streamId", streamId.Id));

                var maxCount = default(int?);
                using (var reader = await command.ExecuteReaderAsync())
                {
                    var jsonData = reader.GetString(0);
                    var metadataMessage = SimpleJson.DeserializeObject<MetadataMessage>(jsonData);
                    maxCount = metadataMessage.MaxCount;
                }

                return new SQLiteAppendResult(maxCount, latestStreamVersion.Value, latestStreamPosition.Value);
           }
        }

        public static async Task<int> GetStreamVersionOfMessageId(
            this SQLiteConnection connection, 
            SQLiteTransaction transaction,
            SQLiteStreamId streamId,
            Guid messageId,
            CancellationToken cancellationToken)
        {
            using (var command = connection.CreateCommand())
            {
                command.Transaction = transaction;
                command.CommandText = @"SELECT TOP 1 messages.stream_version
FROM messages
where messages.stream_id_internal = (SELECT streams.id_internal FROM streams WHERE streams.id = @streamId) AND
  messages.id = @messageId;";
                command.Parameters.Add(new SQLiteParameter("@streamId", streamId.Id));
                command.Parameters.Add(new SQLiteParameter("@messageId", messageId));

                var result = await command.ExecuteScalarAsync(cancellationToken).NotOnCapturedContext();

                return (int)result;
            }            
        }
    }
}