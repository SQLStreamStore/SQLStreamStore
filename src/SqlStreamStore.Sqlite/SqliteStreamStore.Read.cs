namespace SqlStreamStore
{
    using System;
    using System.Collections.Generic;
    using System.Data;
    using System.Linq;
    using System.Threading;
    using System.Threading.Tasks;
    using Microsoft.Data.Sqlite;
    using SqlStreamStore.Streams;

    public partial class SqliteStreamStore
    {
        protected override Task<ReadStreamPage> ReadStreamForwardsInternal(
            string streamId,
            int fromVersion,
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
            
            using (var connection = OpenConnection())
            using (var command = connection.CreateCommand())
            {
                var idInfo = new StreamIdInfo(streamId);
                int idInternal = 0;
                var maxAge = default(int?);
                command.CommandText = @"SELECT streams.id_internal, streams.max_age, streams.max_count
                                        FROM streams
                                            LEFT JOIN messages ON messages.stream_id_internal = streams.id_internal 
                                        WHERE 
                                            streams.id_original = @idOriginal
                                        LIMIT 1;";
                command.Parameters.Clear();
                command.Parameters.AddWithValue("@idOriginal", idInfo.SqlStreamId.IdOriginal);
                using(var reader = command.ExecuteReader(CommandBehavior.SingleRow))
                {
                    if(!reader.Read())
                    {
                        // not found.
                        return Task.FromResult(new ReadStreamPage(
                            streamId,
                            PageReadStatus.StreamNotFound,
                            fromVersion,
                            StreamVersion.End,
                            StreamVersion.End,
                            StreamVersion.End,
                            ReadDirection.Forward,
                            true,
                            readNext));
                    }

                    idInternal = reader.ReadScalar<int>(0);
                    maxAge = reader.ReadScalar(1, default(int?));

                    var streamsMaxRecords = reader.ReadScalar(2, StreamVersion.Start);
                    maxRecords = Math.Min(maxRecords, (streamsMaxRecords == StreamVersion.Start ? maxRecords : streamsMaxRecords));
                }

                command.CommandText = @"SELECT MIN(messages.position)
                                    FROM messages
                                    WHERE messages.stream_id_internal = @idInternal
                                        AND messages.stream_version >= @streamVersion;";
                command.Parameters.Clear();
                command.Parameters.AddWithValue("@idInternal", idInternal);
                command.Parameters.AddWithValue("@streamVersion", fromVersion);
                var position = command.ExecuteScalar<long?>();

                if(position == null)
                {
                    command.CommandText = @"SELECT messages.position
                                FROM messages
                                WHERE messages.stream_id_internal = @idInternal
                                    AND messages.stream_version = @streamVersion;";
                    command.Parameters.Clear();
                    command.Parameters.AddWithValue("@idInternal", idInternal);
                    command.Parameters.AddWithValue("@streamVersion", fromVersion);
                    position = command.ExecuteScalar<long?>();
                }
                
                // if no position, then need to return success with end of stream.
                if(position == null)
                {
                    // not found.
                    return Task.FromResult(new ReadStreamPage(
                        streamId,
                        PageReadStatus.Success,
                        fromVersion,
                        StreamVersion.Start,
                        StreamVersion.End,
                        StreamVersion.End,
                        ReadDirection.Forward,
                        true,
                        readNext));
                }

                return PrepareStreamResponse(command, idInfo, ReadDirection.Forward, fromVersion, prefetch, readNext, idInternal, position, maxRecords, maxAge);
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
                var maxAge = default(int?);
                command.CommandText = @"SELECT streams.id_internal, streams.max_age, streams.max_count
                                        FROM streams
                                            LEFT JOIN messages ON messages.stream_id_internal = streams.id_internal 
                                        WHERE 
                                            streams.id_original = @idOriginal
                                        LIMIT 1;";
                command.Parameters.Clear();
                command.Parameters.AddWithValue("@idOriginal", idInfo.SqlStreamId.IdOriginal);
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
                    maxAge = reader.ReadScalar(1, default(int?));

                    var streamsMaxRecords = reader.ReadScalar(2, StreamVersion.End);
                    maxRecords = Math.Min(maxRecords, streamsMaxRecords == StreamVersion.End ? maxRecords : streamsMaxRecords);
                }


                command.CommandText = @"SELECT MAX(messages.position)
                                    FROM messages
                                    WHERE messages.stream_id_internal = @idOriginal
                                        AND messages.stream_version >= @streamVersion;";
                command.Parameters.Clear();
                command.Parameters.AddWithValue("@idOriginal", streamIdInternal);
                command.Parameters.AddWithValue("@streamVersion", fromStreamVersion);
                var position = command.ExecuteScalar<long?>();

                if(position == null)
                {
                    command.CommandText = @"SELECT messages.position
                                FROM messages
                                WHERE messages.stream_id_internal = @idOriginal
                                    AND messages.stream_version = @streamVersion;";
                    command.Parameters.Clear();
                    command.Parameters.AddWithValue("@idOriginal", streamIdInternal);
                    command.Parameters.AddWithValue("@streamVersion", fromStreamVersion);
                    position = command.ExecuteScalar<long?>();
                }
                
                // if no position, then need to return success with end of stream.
                if(position == null)
                {
                    // not found.
                    return Task.FromResult(new ReadStreamPage(
                        streamId,
                        PageReadStatus.Success,
                        fromStreamVersion,
                        StreamVersion.End,
                        StreamVersion.End,
                        StreamVersion.End,
                        ReadDirection.Backward,
                        true,
                        readNext));
                }

                return PrepareStreamResponse(command, idInfo, ReadDirection.Backward, fromStreamVersion, prefetch, readNext, streamIdInternal, position, maxRecords, maxAge);
            }
        }

        private Task<ReadStreamPage> PrepareStreamResponse(
            SqliteCommand command,
            StreamIdInfo idInfo,
            ReadDirection direction,
            int fromVersion,
            bool prefetch,
            ReadNextStreamPage readNext,
            int streamIdInternal,
            long? position,
            int maxRecords,
            int? maxAge)
        {
            command.CommandText = @"SELECT COUNT(*)
                        FROM messages 
                        WHERE messages.stream_id_internal = @idInternal 
                        AND CASE 
                                WHEN @forwards THEN messages.[position] >= @position
                                ELSE messages.[position] <= @position
                            END; -- count of remaining messages.
                        
                        SELECT MAX(messages.stream_version) 
                        FROM messages 
                        WHERE messages.stream_id_internal = @idInternal; -- the highest version of the stream (the end of it.)
                         
                        SELECT messages.event_id,
                               messages.stream_version,
                               messages.[position],
                               messages.created_utc,
                               messages.[type],
                               messages.json_metadata,
                               case when @prefetch then messages.json_data else null end as json_data
                        FROM messages
                        WHERE messages.stream_id_internal = @idInternal 
                        AND CASE 
                                WHEN @forwards THEN messages.[position] >= @position
                                ELSE messages.[position] <= @position
                            END
                        ORDER BY
                            CASE 
                                WHEN @forwards THEN messages.position
                                ELSE -messages.position
                            END
                        LIMIT @count; -- messages";
            command.Parameters.Clear();
            command.Parameters.AddWithValue("@idInternal", streamIdInternal);
            command.Parameters.AddWithValue("@position", position);
            command.Parameters.AddWithValue("@prefetch", prefetch);
            command.Parameters.AddWithValue("@count", maxRecords);
            command.Parameters.AddWithValue("@forwards", direction == ReadDirection.Forward);

            var messages = new List<(StreamMessage message, int? maxAge)>();
            using(var reader = command.ExecuteReader())
            {
                reader.Read();
                var remainingMessages = Convert.ToInt32(reader.GetValue(0));

                reader.NextResult();
                reader.Read();
                var endOfStreamVersionNumber = reader.ReadScalar<int>(0);

                reader.NextResult();
                while(reader.Read())
                {
                    messages.Add((ReadStreamMessage(reader, idInfo, prefetch), maxAge));
                }

                bool isEnd;
                int nextVersion;
                var filtered = FilterExpired(messages);
                
                if(direction == ReadDirection.Forward)
                {
                    isEnd =  Math.Min(remainingMessages, maxRecords) - messages.Count <= 0
                             &&
                             remainingMessages <= maxRecords;
                    //isEnd = !messages.Any() || messages.Any(m => m.message.Position == position);
                    nextVersion = messages.Any() ? messages.Last().message.StreamVersion + 1 : StreamVersion.Start; 
                }
                else
                {
                    isEnd =  Math.Min(remainingMessages, maxRecords) - messages.Count <= 0
                             &&
                             remainingMessages <= maxRecords;
                    nextVersion = messages.Any() ? messages.Last().message.StreamVersion - 1 : StreamVersion.End;
                }
                

                var page = new ReadStreamPage(
                    idInfo.SqlStreamId.IdOriginal,
                    PageReadStatus.Success,
                    fromVersion,
                    nextVersion,
                    endOfStreamVersionNumber,
                    position.Value,
                    direction,
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