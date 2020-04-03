namespace SqlStreamStore
{
    using System;
    using System.Collections.Generic;
    using System.Data;
    using System.Data.Common;
    using System.Linq;
    using System.Threading;
    using System.Threading.Tasks;
    using Microsoft.Data.Sqlite;
    using SqlStreamStore.Streams;

    public partial class SQLiteStreamStore
    {
        protected override Task<ReadAllPage> ReadAllForwardsInternal(long fromPositionExclusive, int maxCount, bool prefetch, ReadNextAllPage readNext, CancellationToken cancellationToken)
        {
            var position = fromPositionExclusive;
            maxCount = maxCount == int.MaxValue ? maxCount - 1 : maxCount;

            using (var connection = OpenConnection())
            {
                using (var command = connection.CreateCommand())
                {
                    command.CommandText = @"SELECT streams.id_original As stream_id,
            messages.stream_version,
            messages.position,
            messages.message_id,
            messages.createdUtc,
            messages.type,
            messages.json_metadata,
            CASE WHEN @includeJsonData = true THEN messages.json_data ELSE null END
       FROM messages
 INNER JOIN streams
         ON messages.stream_id_internal = streams.id_internal
      WHERE messages.position >= @position
   ORDER BY messages.position
      LIMIT @count;";
                    command.Parameters.AddWithValue("@position", position);
                    command.Parameters.AddWithValue("@count", maxCount + 1);
                    command.Parameters.AddWithValue("@includeJsonData", prefetch);
                    var reader = command
                        .ExecuteReader(CommandBehavior.SequentialAccess);
                    
                    var messages = new List<StreamMessage>();
                    if (!reader.HasRows)
                    {
                        return Task.FromResult(new ReadAllPage(
                            fromPositionExclusive,
                            fromPositionExclusive,
                            true,
                            ReadDirection.Forward,
                            readNext,
                            messages.ToArray()));
                    }

                    while (reader.Read())
                    {
                        if (messages.Count == maxCount)
                        {
                            messages.Add(default);
                        }
                        else
                        {
                            var streamId = reader.GetString(0);
                            var streamVersion = reader.GetInt32(1);
                            position = reader.GetInt64(2);
                            var messageId = reader.GetGuid(3);
                            var created = reader.GetDateTime(4);
                            var type = reader.GetString(5);
                            var jsonMetadata = reader.GetString(6);

                            Func<CancellationToken, Task<string>> getJsonData;
                            if (prefetch)
                            {
                                var jsonData = reader.GetTextReader(7).ReadToEnd();
                                getJsonData = (ct) => Task.FromResult(jsonData);
                            }
                            else
                            {
                                var streamIdInfo = new StreamIdInfo(streamId);
                                getJsonData = ct => Task.FromResult(GetJsonData(command, streamIdInfo.SQLiteStreamId.Id, streamVersion));
                            }

                            var message = new StreamMessage(streamId,
                                messageId,
                                streamVersion,
                                position,
                                created,
                                type,
                                jsonMetadata,
                                getJsonData);

                            messages.Add(message);
                        }
                    }

                    bool isEnd = true;
                    if (messages.Count == maxCount + 1)
                    {
                        isEnd = false;
                        messages.RemoveAt(maxCount);
                    }

                    var nextPosition = messages[messages.Count - 1].Position + 1;

                    return Task.FromResult(new ReadAllPage(
                        fromPositionExclusive,
                        nextPosition,
                        isEnd,
                        ReadDirection.Forward,
                        readNext,
                        messages.ToArray()));
                }
            }
        }

        protected override Task<ReadAllPage> ReadAllBackwardsInternal(
            long fromPositionExclusive, 
            int maxCount, 
            bool prefetch, 
            ReadNextAllPage readNext, 
            CancellationToken cancellationToken)
        {
            maxCount = maxCount == int.MaxValue ? maxCount - 1 : maxCount;
            long position = fromPositionExclusive == Position.End ? long.MaxValue : fromPositionExclusive;

            using (var connection = OpenConnection())
            {
                using (var command = connection.CreateCommand())
                {
                    command.CommandText = @"SELECT streams.id_original As StreamId,
            messages.stream_version,
            messages.position,
            messages.message_id,
            messages.created_utc,
            messages.type,
            messages.json_metadata,
            CASE WHEN @includeJsonData = true THEN messages.json_data ELSE null END
       FROM messages
 INNER JOIN streams
         ON messages.stream_id_internal = streams.id_internal
      WHERE messages.position <= @position
   ORDER BY messages.position DESC
      LIMIT @count;"; 
                    command.Parameters.AddWithValue("@count", maxCount);
                    command.Parameters.AddWithValue("@position", position);
                    command.Parameters.AddWithValue("@includeJsonData", prefetch);
                    var reader = command
                        .ExecuteReader(CommandBehavior.SequentialAccess);

                    var messages = new List<StreamMessage>();
                    if (!reader.HasRows)
                    {
                        // When reading backwards and there are no more items, then next position is LongPosition.Start,
                        // regardless of what the fromPosition is.
                        return Task.FromResult(new ReadAllPage(
                            Position.Start,
                            Position.Start,
                            true,
                            ReadDirection.Backward,
                            readNext,
                            messages.ToArray()));
                    }

                    long lastPosition = 0;
                    while (reader.Read())
                    {
                        var streamId = reader.GetString(0);
                        var streamVersion = reader.GetInt32(1);
                        position = reader.GetInt64(2);
                        var messageId = reader.GetGuid(3);
                        var created = reader.GetDateTime(4);
                        var type = reader.GetString(5);
                        var jsonMetadata = reader.GetString(6);

                        Func<CancellationToken, Task<string>> getJsonData;
                        if (prefetch)
                        {
                            var jsonData = reader.GetTextReader(7).ReadToEnd();
                            getJsonData = _ => Task.FromResult(jsonData);
                        }
                        else
                        {
                            var streamIdInfo = new StreamIdInfo(streamId);
                            getJsonData = ct => Task.FromResult(GetJsonData(command, streamIdInfo.SQLiteStreamId.Id, streamVersion));
                        }

                        var message = new StreamMessage(
                            streamId,
                            messageId,
                            streamVersion,
                            position,
                            created,
                            type,
                            jsonMetadata, getJsonData);

                        messages.Add(message);
                        lastPosition = position;
                    }

                    bool isEnd = true;
                    var nextPosition = lastPosition;

                    if (messages.Count == maxCount + 1) // An extra row was read, we're not at the end
                    {
                        isEnd = false;
                        messages.RemoveAt(maxCount);
                    }

                    fromPositionExclusive = messages.Any() ? messages[0].Position : 0;

                    return Task.FromResult(new ReadAllPage(
                        fromPositionExclusive,
                        nextPosition,
                        isEnd,
                        ReadDirection.Backward,
                        readNext,
                        messages.ToArray()));
                }
            }
        }
    }

    internal static class SqlDataReaderExtensions
    {
        internal static int? GetNullableInt32(this SqliteDataReader reader, int ordinal) 
            => reader.IsDBNull(ordinal) 
                ? (int?) null 
                : reader.GetInt32(ordinal);

        internal static int? GetNullableInt32(this DbDataReader reader, int ordinal) 
            => reader.IsDBNull(ordinal) 
                ? (int?) null 
                : reader.GetInt32(ordinal);
    }
}