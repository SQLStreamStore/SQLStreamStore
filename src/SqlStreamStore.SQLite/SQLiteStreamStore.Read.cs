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

            Func<List<StreamMessage>, int, int> getNextVersion = null;
            if (direction == ReadDirection.Forward)
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

            using (var command = connection.CreateCommand())
            {
                command.CommandText = @"SELECT streams.id_internal, streams.version, streams.position
       FROM streams
      WHERE streams.id = @streamId";
                command.Parameters.AddWithValue("@streamId", sqlStreamId.Id);

                var streamIdInternal = default(int?);
                var streamVersion = -1;
                var streamPosition = default(int?);

                using (var reader = command.ExecuteReader())
                {
                    if(reader.Read())
                    {
                        streamIdInternal = reader.GetNullableInt32(0);
                        streamVersion = reader.GetInt32(1);
                        streamPosition = reader.GetNullableInt32(2);
                    }
                    else
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
                }

                if (streamPosition == null)
                {
                    command.CommandText = @"SELECT min(messages.position)
                    FROM messages
                    WHERE messages.stream_id_internal = @streamIdInternal
                        AND messages.stream_version >= @streamVersion";
                    command.Parameters.Clear();
                    command.Parameters.AddWithValue("@streamIdInternal", streamIdInternal);
                    command.Parameters.AddWithValue("@streamVersion", streamVersion);
                    var pos = command.ExecuteScalar();
                    streamPosition = pos == DBNull.Value ? 0 : (int?) pos;
                }

                command.CommandText = @"SELECT messages.stream_version,
            messages.[position],
            messages.message_id AS event_id,
            messages.created_utc,
            messages.[type],
            messages.json_metadata,
            CASE WHEN @includeJsonData = true THEN messages.json_data ELSE null END as json_data
       FROM messages
      WHERE messages.stream_id_internal = @streamIdInternal AND messages.position >= @position
   ORDER BY messages.position
      LIMIT @count;";
                command.Parameters.Clear();
                command.Parameters.AddWithValue("@streamIdInternal", streamIdInternal);
                command.Parameters.AddWithValue("@count", count + 1 );
                command.Parameters.AddWithValue("@position", streamPosition);
                command.Parameters.AddWithValue("@includeJsonData", prefetch);

                using (var reader = command.ExecuteReader(CommandBehavior.SequentialAccess))
                {
                    var messages = new List<StreamMessage>();
                    while (reader.Read())
                    {
                        if (messages.Count == count)
                        {
                            messages.Add(default(StreamMessage));
                        }
                        else
                        {
                            var streamVersion1 = reader.GetInt32(0);
                            var ordinal = reader.GetInt64(1);
                            var eventId = reader.GetGuid(2);
                            var created = reader.GetDateTime(3);
                            var type = reader.GetString(4);
                            var jsonMetadata = reader.GetString(5);

                            Func<CancellationToken, Task<string>> getJsonData;
                            if(prefetch)
                            {
                                var jsonData = reader.GetTextReader(6).ReadToEnd();
                                getJsonData = _ => Task.FromResult(jsonData);
                            }
                            else
                            {
                                getJsonData = ct => Task.FromResult(GetJsonData(command, sqlStreamId.Id, streamVersion1));
                            }

                            var message = new StreamMessage(
                                sqlStreamId.IdOriginal,
                                eventId,
                                streamVersion1,
                                ordinal,
                                created,
                                type,
                                jsonMetadata,
                                getJsonData
                            );

                            messages.Add(message);
                        }
                    }

                    var isEnd = true;
                    if (messages.Count == count + 1)
                    {
                        isEnd = false;
                        messages.RemoveAt(count);
                    }

                    return new ReadStreamPage(
                        sqlStreamId.IdOriginal,
                        PageReadStatus.Success,
                        start,
                        getNextVersion(messages, streamVersion),
                        streamVersion,
                        streamPosition.Value,
                        direction,
                        isEnd,
                        readNext,
                        messages.ToArray());
                }
            }
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