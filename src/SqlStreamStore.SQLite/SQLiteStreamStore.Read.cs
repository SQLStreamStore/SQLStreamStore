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
        protected override Task<ReadStreamPage> ReadStreamForwardsInternal(
            string streamId, 
            int start, 
            int count, 
            bool prefetch, 
            ReadNextStreamPage readNext, 
            CancellationToken cancellationToken)
        {
            using (var connection = OpenConnection())
            {
                var streamIdInfo = new StreamIdInfo(streamId);
                return Task.FromResult(ReadStreamInternal(
                    streamIdInfo.SQLiteStreamId,
                    start,
                    count,
                    ReadDirection.Forward,
                    prefetch,
                    readNext,
                    connection,
                    null,
                    cancellationToken));
            }
        }

        protected override Task<ReadStreamPage> ReadStreamBackwardsInternal(string streamId, int fromVersionInclusive, int count, bool prefetch, ReadNextStreamPage readNext, CancellationToken cancellationToken)
        {
            using (var connection = OpenConnection())
            {
                var streamIdInfo = new StreamIdInfo(streamId);
                return Task.FromResult(ReadStreamInternal(
                    streamIdInfo.SQLiteStreamId,
                    fromVersionInclusive,
                    count,
                    ReadDirection.Backward,
                    prefetch,
                    readNext,
                    connection,
                    null,
                    cancellationToken));
            }
        }
        private ReadStreamPage ReadStreamInternal(
            SQLiteStreamId sqlStreamId,
            int start,
            int count,
            ReadDirection direction,
            bool prefetch,
            ReadNextStreamPage readNext,
            SqliteConnection connection,
            SqliteTransaction transaction,
            CancellationToken cancellationToken)
        {
            count = count == int.MaxValue ? count - 1 : count;
            var streamVersion = start == StreamVersion.End ? int.MaxValue : start;
            var messages = new List<(StreamMessage message, int? maxAge)>();
            Func<List<StreamMessage>, int, int> getNextVersion = null;
            
            if (direction == ReadDirection.Forward)
            {
                getNextVersion = (events, lastVersion) =>
                {
                    if(events.Any())
                    {
                        var currentVersion = events.Last().StreamVersion;
                        return currentVersion + 1;
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
                        var currentVersion = events.Last().StreamVersion;
                        return currentVersion - 1;
                    }
                    
                    return -1;
                };
            }

            using (var command = connection.CreateCommand())
            {
                command.CommandText = @"SELECT id_internal FROM streams WHERE id = @streamId";
                command.Parameters.AddWithValue("@streamId", sqlStreamId.Id);
                var streamIdInternal = Convert.ToInt32(command.ExecuteScalar());

                command.CommandText = @"SELECT streams.[version], streams.[position], streams.max_age
                                        FROM streams
                                        WHERE streams.id_internal = @streamIdInternal;
                                        
                                        SELECT streams.id_original as stream_id,
                                               messages.message_id,
                                               messages.stream_version,
                                               (messages.[position] - 1),
                                               messages.created_utc,
                                               messages.type,
                                               messages.json_metadata,
                                               (CASE
                                                  WHEN @prefetch THEN messages.json_data
                                                  ELSE NULL 
                                               END) as json_data
                                        FROM messages
                                               INNER JOIN streams ON messages.stream_id_internal = streams.id_internal
                                        WHERE (CASE
                                                 WHEN @forwards THEN messages.stream_version >= @version AND id_internal = @streamIdInternal
                                                 ELSE messages.stream_version <= @version AND streams.id_internal = @streamIdInternal
                                               END)
                                        ORDER BY (CASE
                                                    WHEN @forwards THEN messages.stream_version
                                                    ELSE messages.stream_version * -1
                                                  END)
                                        LIMIT @count;";
                command.Parameters.Clear();
                command.Parameters.AddWithValue("@streamIdInternal", streamIdInternal);
                command.Parameters.AddWithValue("@prefetch", prefetch);
                command.Parameters.AddWithValue("@forwards", direction == ReadDirection.Forward);
                command.Parameters.AddWithValue("@version", streamVersion);
                command.Parameters.AddWithValue("@count", count);

                using(var reader = command.ExecuteReader())
                {
                    if(!reader.HasRows)
                    {
                        return new ReadStreamPage(
                            sqlStreamId.IdOriginal,
                            PageReadStatus.StreamNotFound,
                            start,
                            -1,
                            -1,
                            -1,
                            direction,
                            true,
                            readNext);
                    }

                    if(messages.Count == count)
                    {
                        messages.Add(default);
                    }

                    reader.Read();
                    var lastVersion = reader.GetInt32(0);
                    var lastPosition = reader.GetInt64(1);
                    var maxAge = reader.IsDBNull(2)
                        ? default
                        : reader.GetInt32(2);

                    reader.NextResult();

                    while(reader.Read())
                    {
                        messages.Add((ReadStreamMessage(command, reader, sqlStreamId, prefetch), maxAge));
                    }

                    var isEnd = true;
                    if(messages.Count == count + 1)
                    {
                        isEnd = false;
                        messages.RemoveAt(count);
                    }

                    var filteredMessages = FilterExpired(messages);

                    return new ReadStreamPage(
                        sqlStreamId.IdOriginal,
                        PageReadStatus.Success,
                        start,
                        getNextVersion(filteredMessages, lastVersion),
                        lastVersion,
                        lastPosition,
                        direction,
                        isEnd,
                        readNext,
                        filteredMessages.ToArray());
                }
            }
        }

        private StreamMessage ReadStreamMessage(SqliteCommand command, SqliteDataReader reader, SQLiteStreamId sqlStreamId, bool prefetch)
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
            
            var messageId = reader.GetGuid(1);
            var streamVersion = reader.GetInt32(2);
            var position = reader.GetInt64(3);
            var createdUtc = reader.GetDateTime(4);
            var type = reader.GetString(5);
            var jsonMetadata = ReadString(6);

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
                    ReadString(7));
            }

            return new StreamMessage(
                sqlStreamId.IdOriginal,
                messageId,
                streamVersion,
                position,
                createdUtc,
                type,
                jsonMetadata,
                ct => Task.FromResult(GetJsonData(command, sqlStreamId.Id, streamVersion)));
            
        }

        private string GetJsonData(SqliteCommand command, string streamId, int streamVersion)
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