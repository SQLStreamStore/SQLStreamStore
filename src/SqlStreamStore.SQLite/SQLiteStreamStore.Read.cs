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

    public partial class SQLiteStreamStore
    {
        private static readonly ReadNextStreamPage s_readNextNotFound =
            (_, ct) => throw new InvalidOperationException("Cannot read next page of non-exisitent stream");

        protected override Task<ReadStreamPage> ReadStreamForwardsInternal(
            string streamId, 
            int start, 
            int count, 
            bool prefetch, 
            ReadNextStreamPage readNext, 
            CancellationToken cancellationToken)
        {
            return Task.FromResult(ReadStreamInternal(
                streamId,
                start,
                count,
                ReadDirection.Forward,
                prefetch,
                readNext,
                cancellationToken));
        }

        protected override Task<ReadStreamPage> ReadStreamBackwardsInternal(string streamId, int fromVersionInclusive, int count, bool prefetch, ReadNextStreamPage readNext, CancellationToken cancellationToken)
        {
                return Task.FromResult(ReadStreamInternal(
                    streamId,
                    fromVersionInclusive,
                    count,
                    ReadDirection.Backward,
                    prefetch,
                    readNext,
                    cancellationToken));
        }
        
        private ReadStreamPage ReadStreamInternal(
            string streamId,
            int start,
            int count,
            ReadDirection direction,
            bool prefetch,
            ReadNextStreamPage readNext,
            CancellationToken cancellationToken)
        {
            GuardAgainstDisposed();
            cancellationToken.ThrowIfCancellationRequested();

            // If the count is int.MaxValue, TSql will see it as a negative number. 
            // Users shouldn't be using int.MaxValue in the first place anyway.
            //count = count == int.MaxValue ? count - 1 : count;
            count = 2;

            var streamVersion = start == StreamVersion.End ? int.MaxValue : start;
            Func<List<StreamMessage>, int, int> getNextVersion;

            if(direction == ReadDirection.Forward)
            {
                getNextVersion = (events, lastVersion) =>
                {
                    if(events.Any())
                    {
                        return events.Last().StreamVersion + 1;
                    }
                    return lastVersion + 1;
                };
            }
            else
            {
                getNextVersion = (events, lastVersion) =>
                {
                    if (events.Any())
                    {
                        return events.Last().StreamVersion - 1;
                    }
                    return -1;
                };
            }

            using (var connection = OpenConnection())
            using (var command = connection.CreateCommand())
            {
                int streamIdInternal = 0;
                int lastStreamVersion = 0;
                int lastStreamPosition = 0;
                var maxAge = default(int?);
                command.CommandText = @"SELECT streams.id_internal, streams.[version], streams.[position], max_age, max_count
                                        FROM streams 
                                        where streams.id_original = @streamIdOriginal";
                command.Parameters.Clear();
                command.Parameters.AddWithValue("@streamIdOriginal", streamId);
                using(var reader = command.ExecuteReader())
                {
                    if(!reader.Read())
                    {
                        // not found.
                        return new ReadStreamPage(
                            streamId,
                            PageReadStatus.StreamNotFound,
                            start,
                            StreamVersion.End,
                            StreamVersion.End,
                            StreamVersion.End,
                            direction,
                            true,
                            readNext);
                    }

                    streamIdInternal = reader.GetInt32(0);
                    lastStreamVersion = reader.GetInt32(1);
                    lastStreamPosition = reader.GetInt32(2);
                    maxAge = reader.IsDBNull(3) ? default(int?) : reader.GetInt32(3);
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
                    command.Parameters.AddWithValue("@streamVersion", lastStreamVersion);
                    position = command.ExecuteScalar<long?>() ?? Position.Start;
                }

                command.CommandText = direction == ReadDirection.Forward
                    ? @"SELECT COUNT(*) 
                        FROM messages 
                        WHERE messages.stream_id_internal = @streamIdInternal AND position >= @position; 
                        SELECT messages.message_id,
                               messages.stream_version,
                               messages.[position],
                               messages.created_utc,
                               messages.type,
                               messages.json_metadata,
                               case when @prefetch then messages.json_data else null end as json_data
                        FROM messages
                        WHERE messages.stream_id_internal = @streamIdInternal AND messages.position >= @position
                        ORDER BY messages.position
                        LIMIT @count;"
                    : @"SELECT COUNT(*) 
                        FROM messages 
                        WHERE messages.stream_id_internal = @streamIdInternal AND position <= @position; 
                        SELECT messages.message_id,
                               messages.stream_version,
                               messages.[position],
                               messages.created_utc,
                               messages.type,
                               messages.json_metadata,
                               case when @prefetch then messages.json_data else null end as json_data
                        FROM messages
                        WHERE messages.stream_id_internal = @streamIdInternal AND messages.position <= @position
                        ORDER BY messages.position DESC
                        LIMIT @count;";
                command.Parameters.Clear();
                command.Parameters.AddWithValue("@streamIdInternal", streamIdInternal);
                command.Parameters.AddWithValue("@position", position);
                command.Parameters.AddWithValue("@prefetch", prefetch);
                command.Parameters.AddWithValue("@count", count);

                var messages = new List<(StreamMessage message, int? maxAge)>();
                using(var reader = command.ExecuteReader())
                {
                    reader.Read();
                    var remainingMessages = Convert.ToInt32(reader.GetValue(0));

                    reader.NextResult();
                    while(reader.Read())
                    {
                        messages.Add((ReadStreamMessage(reader, new SQLiteStreamId(streamId), prefetch), maxAge));
                    }

                    var filtered = FilterExpired(messages);
                    
                    var page = new ReadStreamPage(
                        streamId,
                        PageReadStatus.Success,
                        start,
                        getNextVersion(filtered, lastStreamVersion),
                        lastStreamVersion,
                        lastStreamPosition,
                        direction,
                        remainingMessages - messages.Count <= 0, //when true, then end.  when false, then more messages,
                        readNext,
                        filtered.ToArray());
            
                    return page;
                }
            }
        }

        private StreamMessage ReadStreamMessage(SqliteDataReader reader, SQLiteStreamId sqlStreamId, bool prefetch)
        {
            string ReadString(int ordinal)
            {
                if(reader.IsDBNull(ordinal))
                {
                    return null;
                }

                using(var textReader = reader.GetTextReader(ordinal))
                {
                    return textReader.ReadToEnd();
                }
            }

            var messageId = reader.IsDBNull(0) 
                ? Guid.Empty 
                : reader.GetGuid(0);
            var streamVersion = reader.GetInt32(1);
            var position = reader.GetInt64(2);
            var createdUtc = reader.GetDateTime(3);
            var type = reader.GetString(4);
            var jsonMetadata = ReadString(5);

            if(prefetch)
            {
                return new StreamMessage(
                    sqlStreamId.IdOriginal,
                    messageId,
                    streamVersion,
                    position,
                    createdUtc,
                    type,
                    jsonMetadata,
                    ReadString(6));
            }

            return new StreamMessage(
                sqlStreamId.IdOriginal,
                messageId,
                streamVersion,
                position,
                createdUtc,
                type,
                jsonMetadata,
                ct => Task.FromResult(GetJsonData(sqlStreamId.Id, streamVersion)));
            
        }

        private string GetJsonData(string streamId, int streamVersion)
        {
            using (var connection = OpenConnection())
            using(var command = connection.CreateCommand())
            {
                command.CommandText = @"SELECT messages.json_data
FROM messages
WHERE messages.stream_id_internal = 
(
SELECT streams.id_internal
FROM streams
WHERE streams.id = @streamId)
AND messages.stream_version = @streamVersion";
                command.Parameters.Clear();
                command.Parameters.AddWithValue("@streamId", streamId);
                command.Parameters.AddWithValue("@streamVersion", streamVersion);

                using(var reader = command
                    .ExecuteReader(
                        CommandBehavior.SequentialAccess | CommandBehavior.SingleRow))
                {
                    if(reader.Read())
                    {
                        return reader.GetTextReader(0).ReadToEnd();
                    }

                    return null;
                }
            }
        }
    }
}