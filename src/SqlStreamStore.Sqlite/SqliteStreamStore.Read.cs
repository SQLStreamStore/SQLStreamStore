namespace SqlStreamStore
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Threading;
    using System.Threading.Tasks;
    using Microsoft.Data.Sqlite;
    using SqlStreamStore.Streams;

    public partial class SqliteStreamStore
    {
        protected override Task<ReadStreamPage> ReadStreamForwardsInternal(
            string streamId,
            int fromStreamVersion,
            int count,
            bool prefetch,
            ReadNextStreamPage readNext,
            CancellationToken cancellationToken)
        {
            GuardAgainstDisposed();
            cancellationToken.ThrowIfCancellationRequested();

            // If the count is int.MaxValue, TSql will see it as a negative number. 
            // Users shouldn't be using int.MaxValue in the first place anyway.
            var maxRecords = count == int.MaxValue ? count - 1 : count;
            var streamVersion = fromStreamVersion;
            if(streamVersion < StreamVersion.Start) streamVersion = StreamVersion.Start;
            
            using (var connection = OpenConnection())
            using (var command = connection.CreateCommand())
            {
                var idInfo = new StreamIdInfo(streamId);
                int streamIdInternal = 0;
                long lastStreamPosition = 0;
                var maxAge = default(int?);
                command.CommandText = @"SELECT streams.id_internal, 
                                                streams.[version], 
                                                streams.[position], 
                                                coalesce(max_age, (SELECT s.max_age FROM streams s WHERE s.id = @metaId)), 
                                                coalesce(max_count, (SELECT s.max_count FROM streams s WHERE s.id = @metaId))
                                        FROM streams
                                        where streams.id = @streamId";
                command.Parameters.Clear();
                command.Parameters.AddWithValue("@streamId", idInfo.SqlStreamId.Id);
                command.Parameters.AddWithValue("@metaId", idInfo.MetadataSqlStreamId.Id);
                using(var reader = command.ExecuteReader())
                {
                    if(!reader.Read())
                    {
                        // not found.
                        return Task.FromResult(new ReadStreamPage(
                            streamId,
                            PageReadStatus.StreamNotFound,
                            fromStreamVersion,
                            StreamVersion.End,
                            StreamVersion.End,
                            StreamVersion.End,
                            ReadDirection.Forward,
                            true,
                            readNext));
                    }

                    streamIdInternal = reader.ReadScalar<int>(0);
                    lastStreamPosition = reader.ReadScalar<int>(2);
                    maxAge = reader.ReadScalar(3, default(int?));

                    var streamsMaxRecords = reader.ReadScalar(4, StreamVersion.Start);
                    maxRecords = Math.Min(maxRecords, (streamsMaxRecords == StreamVersion.Start ? maxRecords : streamsMaxRecords));
                }

                long? position;
                command.CommandText = @"SELECT messages.position
                                        FROM messages
                                        WHERE messages.stream_id_internal = @streamIdInternal
                                            AND messages.stream_version = @streamVersion;";
                command.Parameters.Clear();
                command.Parameters.AddWithValue("@streamIdInternal", streamIdInternal);
                command.Parameters.AddWithValue("@streamVersion", streamVersion);
                position = command.ExecuteScalar<long?>();

                if(position == null)
                {
                    command.CommandText = @"SELECT MIN(messages.position)
                                            FROM messages
                                            WHERE messages.stream_id_internal = @streamIdInternal
                                                AND messages.stream_version >= @streamVersion;";
                    command.Parameters.Clear();
                    command.Parameters.AddWithValue("@streamIdInternal", streamIdInternal);
                    command.Parameters.AddWithValue("@streamVersion", streamVersion);
                    position = command.ExecuteScalar<long?>() ?? Position.Start;
                }

                command.CommandText = @"SELECT COUNT(*)
                        FROM messages 
                        WHERE messages.stream_id_internal = @idInternal; -- count of total messages within stream.
                        
                        SELECT MAX(position)
                        FROM messages
                        WHERE messages.stream_id_internal = @idInternal; -- count of all prior messages that have been read.
                        
                        SELECT MAX(stream_version)
                        FROM messages
                        WHERE messages.stream_id_internal = @idInternal; -- the current stream version.
                        
                        SELECT messages.event_id,
                               messages.stream_version,
                               messages.[position],
                               messages.created_utc,
                               messages.type,
                               messages.json_metadata,
                               case when @prefetch then messages.json_data else null end as json_data
                        FROM messages
                        WHERE messages.stream_id_internal = @idInternal AND messages.[position] >= @position
                        ORDER BY messages.position
                        LIMIT @count;";
                command.Parameters.Clear();
                command.Parameters.AddWithValue("@idInternal", streamIdInternal);
                command.Parameters.AddWithValue("@position", position);
                command.Parameters.AddWithValue("@prefetch", prefetch);
                command.Parameters.AddWithValue("@count", maxRecords);

                var messages = new List<(StreamMessage message, int? maxAge)>();
                using(var reader = command.ExecuteReader())
                {
                    reader.Read();
                    var allMessageCount = reader.ReadScalar<int>(0);

                    reader.NextResult();
                    reader.Read();
                    var lastPosition = reader.ReadScalar<int>(0);

                    reader.NextResult();
                    reader.Read();
                    var lastStreamVersion = reader.ReadScalar(0, StreamVersion.End);

                    reader.NextResult();
                    while(reader.Read())
                    {
                        messages.Add((ReadStreamMessage(reader, idInfo, prefetch), maxAge));
                    }

                    var filtered = FilterExpired(messages);
                    var isEnd = !messages.Any() || messages.Any(m => m.message.Position == lastPosition);
                    var nextVersion = messages.Any() ? messages.Last().message.StreamVersion + 1 : StreamVersion.Start; 
                    
                    var page = new ReadStreamPage(
                        streamId,
                        PageReadStatus.Success,
                        fromStreamVersion,
                        nextVersion,
                        lastStreamVersion,
                        lastStreamPosition,
                        ReadDirection.Forward,
                        isEnd,
                        readNext,
                        filtered.ToArray());
            
                    return Task.FromResult(page);
                }
            }
        }

        protected override Task<ReadStreamPage> ReadStreamBackwardsInternal(
            string streamId,
            int fromStreamVersion,
            int count,
            bool prefetch,
            ReadNextStreamPage readNext,
            CancellationToken cancellationToken)
        {
            GuardAgainstDisposed();
            cancellationToken.ThrowIfCancellationRequested();

            // If the count is int.MaxValue, TSql will see it as a negative number. 
            // Users shouldn't be using int.MaxValue in the first place anyway.
            var maxRecords = count == int.MaxValue ? count - 1 : count;
            var streamVersion = fromStreamVersion == StreamVersion.End ? int.MaxValue -1 : fromStreamVersion;
            using (var connection = OpenConnection())
            using (var command = connection.CreateCommand())
            {
                var idInfo = new StreamIdInfo(streamId);
                int streamIdInternal = 0;
                int startStreamVersion = streamVersion;
                int lastStreamVersion = 0;
                long lastStreamPosition = 0;
                var maxAge = default(int?);
                command.CommandText = @"SELECT streams.id_internal, 
                                                streams.[version], 
                                                streams.[position],
                                                max_age,
                                                max_count
                                        FROM streams
                                        where streams.id = @streamId";
                command.Parameters.Clear();
                command.Parameters.AddWithValue("@streamId", idInfo.SqlStreamId.Id);
                using(var reader = command.ExecuteReader())
                {
                    if(!reader.Read())
                    {
                        // not found.
                        return Task.FromResult(new ReadStreamPage(
                            streamId,
                            PageReadStatus.StreamNotFound,
                            startStreamVersion,
                            StreamVersion.End,
                            StreamVersion.End,
                            StreamVersion.End,
                            ReadDirection.Backward,
                            true,
                            readNext));
                    }

                    streamIdInternal = reader.GetInt32(0);
                    lastStreamVersion = reader.GetInt32(1);
                    lastStreamPosition = reader.GetInt32(2);
                    maxAge = reader.ReadScalar(3, default(int?));

                    var streamsMaxRecords = reader.ReadScalar(4, StreamVersion.End);
                    maxRecords = Math.Min(maxRecords, (streamsMaxRecords == StreamVersion.End ? maxRecords : streamsMaxRecords));
                }

                command.CommandText = @"SELECT messages.position
                                        FROM messages
                                        WHERE messages.stream_id_internal = @streamIdInternal
                                            AND messages.stream_version = @streamVersion;";
                command.Parameters.Clear();
                command.Parameters.AddWithValue("@streamIdInternal", streamIdInternal);
                command.Parameters.AddWithValue("@streamVersion", streamVersion);
                var position = command.ExecuteScalar<long?>();

                if(position == null)
                {
                    command.CommandText = @"SELECT MAX(messages.position)
                                            FROM messages
                                            WHERE messages.stream_id_internal = @streamIdInternal;";
                    command.Parameters.Clear();
                    command.Parameters.AddWithValue("@streamIdInternal", streamIdInternal);
                    position = command.ExecuteScalar<long?>(Position.Start);
                }

                return PrepareStreamResponse(streamId, fromStreamVersion, prefetch, readNext, command, streamIdInternal, position, maxRecords, idInfo, maxAge, lastStreamPosition);
            }
        }

        private Task<ReadStreamPage> PrepareStreamResponse(
            string streamId,
            int fromStreamVersion,
            bool prefetch,
            ReadNextStreamPage readNext,
            SqliteCommand command,
            int streamIdInternal,
            long? position,
            int maxRecords,
            StreamIdInfo idInfo,
            int? maxAge,
            long lastStreamPosition)
        {
            int lastStreamVersion;
            command.CommandText = @"SELECT COUNT(*)
                        FROM messages 
                        WHERE messages.stream_id_internal = @idInternal AND messages.[position] <= @position; -- remaining messages.
                         
                        SELECT messages.event_id,
                               messages.stream_version,
                               messages.[position],
                               messages.created_utc,
                               messages.[type],
                               messages.json_metadata,
                               case when @prefetch then messages.json_data else null end as json_data
                        FROM messages
                        WHERE messages.stream_id_internal = @idInternal AND messages.[position] <= @position
                        ORDER BY messages.position DESC
                        LIMIT @count;";
            command.Parameters.AddWithValue("@idInternal", streamIdInternal);
            command.Parameters.AddWithValue("@position", position);
            command.Parameters.AddWithValue("@prefetch", prefetch);
            command.Parameters.AddWithValue("@count", maxRecords);

            var messages = new List<(StreamMessage message, int? maxAge)>();
            using(var reader = command.ExecuteReader())
            {
                reader.Read();
                var remainingMessages = Convert.ToInt32(reader.GetValue(0));

                reader.NextResult();
                while(reader.Read())
                {
                    messages.Add((ReadStreamMessage(reader, idInfo, prefetch), maxAge));
                }

                var filtered = FilterExpired(messages);
                var isEnd =
                    Math.Min(remainingMessages, maxRecords) - messages.Count <= 0
                    &&
                    remainingMessages <= maxRecords;
                var nextVersion = messages.Any() ? messages.Last().message.StreamVersion - 1 : StreamVersion.End;
                lastStreamVersion = messages.Any() ? messages.First().message.StreamVersion : StreamVersion.End;

                var page = new ReadStreamPage(
                    streamId,
                    PageReadStatus.Success,
                    fromStreamVersion,
                    nextVersion,
                    lastStreamVersion,
                    lastStreamPosition,
                    ReadDirection.Backward,
                    isEnd,
                    readNext,
                    filtered.ToArray());

                return Task.FromResult(page);
            }
        }

        private StreamMessage ReadStreamMessage(SqliteDataReader reader, StreamIdInfo idInfo, bool prefetch)
        {
            var messageId = reader.IsDBNull(0) 
                ? Guid.Empty 
                : reader.GetGuid(0);
            var streamVersion = reader.GetInt32(1);
            var position = reader.GetInt64(2);
            var createdUtc = reader.GetDateTime(3);
            var type = reader.GetString(4);
            var jsonMetadata = reader.IsDBNull(5) ? default : reader.GetString(5);
            var preloadJson = (!reader.IsDBNull(6) && prefetch)
                ? reader.GetTextReader(6).ReadToEnd()
                : default;

            return new StreamMessage(
                idInfo.SqlStreamId.IdOriginal,
                messageId,
                streamVersion,
                position,
                createdUtc,
                type,
                jsonMetadata,
                ct => prefetch
                    ? Task.FromResult(preloadJson)
                    : GetJsonData(idInfo.SqlStreamId.Id, streamVersion));
        }
    }
}