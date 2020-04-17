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
                int idInternal = 0;
                var maxAge = default(int?);
                command.CommandText = @"SELECT streams.id_internal, streams.max_age, streams.max_count
                                        FROM streams
                                            LEFT JOIN messages ON messages.stream_id_internal = streams.id_internal 
                                        WHERE 
                                            streams.id_original = @idOriginal
                                        LIMIT 1;";
                command.Parameters.Clear();
                command.Parameters.AddWithValue("@idOriginal", streamId);
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

                return PrepareStreamResponse(command, streamId, ReadDirection.Forward, fromVersion, prefetch, readNext, idInternal, maxRecords, maxAge);
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
                int streamIdInternal = 0;
                var maxAge = default(int?);
                command.CommandText = @"SELECT streams.id_internal, streams.max_age, streams.max_count
                                        FROM streams
                                            LEFT JOIN messages ON messages.stream_id_internal = streams.id_internal 
                                        WHERE 
                                            streams.id_original = @idOriginal
                                        LIMIT 1;";
                command.Parameters.Clear();
                command.Parameters.AddWithValue("@idOriginal", streamId);
                using(var reader = command.ExecuteReader())
                {
                    if(!reader.Read())
                    {
                        // not found.
                        return Task.FromResult(new ReadStreamPage(
                            streamId,
                            PageReadStatus.StreamNotFound,
                            streamVersion,
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


                command.CommandText = @"SELECT messages.position
                                    FROM messages
                                    WHERE messages.stream_id_internal = @idOriginal
                                        AND messages.stream_version <= @streamVersion
                                    ORDER BY messages.position DESC
                                    LIMIT 1;";
                command.Parameters.Clear();
                command.Parameters.AddWithValue("@idOriginal", streamIdInternal);
                command.Parameters.AddWithValue("@streamVersion", streamVersion);
                var position = command.ExecuteScalar<long?>();

                if(position == null)
                {
                    command.CommandText = @"SELECT streams.position
                                FROM streams
                                WHERE streams.id_internal = @idOriginal;";
                    command.Parameters.Clear();
                    command.Parameters.AddWithValue("@idOriginal", streamIdInternal);
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

                return PrepareStreamResponse(command, 
                    streamId, 
                    ReadDirection.Backward, 
                    fromStreamVersion, 
                    prefetch, 
                    readNext, 
                    streamIdInternal, 
                    maxRecords, 
                    maxAge);
            }
        }

        private Task<ReadStreamPage> PrepareStreamResponse(
            SqliteCommand command,
            string streamId,
            ReadDirection direction,
            int fromVersion,
            bool prefetch,
            ReadNextStreamPage readNext,
            int streamIdInternal,
            int maxRecords,
            int? maxAge)
        {
            var streamVersion = fromVersion == StreamVersion.End ? int.MaxValue -1 : fromVersion;
            var lastPosition = 0L;
            int lastVersion = 0;
            int nextVersion = 0;

            command.CommandText = @"SELECT [position], [version]
                                    FROM streams
                                    WHERE streams.id_internal = @idInternal;";
            command.Parameters.Clear();
            command.Parameters.AddWithValue("@idInternal", streamIdInternal);
            using(var reader = command.ExecuteReader(CommandBehavior.SingleRow))
            {
                if(reader.Read())
                {
                    lastPosition = reader.ReadScalar<long>(0);
                    lastVersion = reader.ReadScalar<int>(1);
                }
            }
            

            command.CommandText = @"SELECT messages.position
                                    FROM messages
                                    WHERE messages.stream_id_internal = @idInternal
                                        AND messages.stream_version = @streamVersion;";
            command.Parameters.Clear();
            command.Parameters.AddWithValue("@idInternal", streamIdInternal);
            command.Parameters.AddWithValue("@streamVersion", streamVersion);
            command.Parameters.AddWithValue("@forwards", direction == ReadDirection.Forward);
            var position = command.ExecuteScalar<long?>();

            if(position == null)
            {
                command.CommandText = @"SELECT CASE
                                            WHEN @forwards THEN MIN(messages.position)
                                            ELSE MAX(messages.position)
                                        END
                                        FROM messages
                                        WHERE messages.stream_id_internal = @idInternal 
                                            AND CASE
                                                WHEN @forwards THEN messages.stream_version >= @streamVersion
                                                ELSE messages.stream_version <= @streamVersion
                                            END;";
                command.Parameters.Clear();
                command.Parameters.AddWithValue("@idInternal", streamIdInternal);
                command.Parameters.AddWithValue("@streamVersion", streamVersion);
                command.Parameters.AddWithValue("@forwards", direction == ReadDirection.Forward);
                position = command.ExecuteScalar<long?>(long.MaxValue);
            }
            
            command.CommandText = @"SELECT COUNT(*)
                        FROM messages 
                        WHERE messages.stream_id_internal = @idInternal 
                        AND CASE 
                                WHEN @forwards THEN messages.[position] >= @position
                                ELSE messages.[position] <= @position
                            END; -- count of remaining messages.
                        
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

            bool isEnd = false;
            var filtered = new List<StreamMessage>();
            int remaining = 0;
            var messages = new List<(StreamMessage message, int? maxAge)>();

            using(var reader = command.ExecuteReader())
            {
                reader.Read();
                remaining = reader.ReadScalar<int>(0);

                reader.NextResult();
                while(reader.Read())
                {
                    messages.Add((ReadStreamMessage(reader, streamId, prefetch), maxAge));
                }
                
                filtered.AddRange(FilterExpired(messages));
            }

            isEnd = remaining - messages.Count <= 0;

            if(direction == ReadDirection.Forward)
            {
                if(messages.Any())
                {
                    nextVersion = messages.Last().message.StreamVersion + 1;
                }
                else
                {
                    nextVersion = lastVersion + 1;
                }
            }
            else if (direction == ReadDirection.Backward)
            {
                if(streamVersion == int.MaxValue - 1 && !messages.Any())
                {
                    nextVersion = StreamVersion.End;
                }

                if(messages.Any())
                {
                    nextVersion = messages.Last().message.StreamVersion - 1;
                }
            }

            var page = new ReadStreamPage(
                streamId,
                status: PageReadStatus.Success,
                fromStreamVersion: fromVersion,
                nextStreamVersion: nextVersion,
                lastStreamVersion: lastVersion,
                lastStreamPosition: lastPosition,
                direction: direction,
                isEnd: isEnd,
                readNext: readNext,
                messages: filtered.ToArray());

            return Task.FromResult(page);
        }

        private StreamMessage ReadStreamMessage(SqliteDataReader reader, string streamId, bool prefetch)
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
                streamId,
                messageId,
                streamVersion,
                position,
                createdUtc,
                type,
                jsonMetadata,
                ct => prefetch
                    ? Task.FromResult(preloadJson)
                    : SqliteCommandExtensions.GetJsonData(streamId, streamVersion));
        }
    }
}