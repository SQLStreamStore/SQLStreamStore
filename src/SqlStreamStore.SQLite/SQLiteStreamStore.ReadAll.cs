namespace SqlStreamStore
{
    using System;
    using System.Collections.Generic;
    using System.Data;
    using System.Data.Common;
    using System.Data.SQLite;
    using System.Linq;
    using System.Threading;
    using System.Threading.Tasks;
    using SqlStreamStore.Infrastructure;
    using SqlStreamStore.Streams;

    public partial class SQLiteStreamStore
    {
        protected override async Task<ReadAllPage> ReadAllForwardsInternal(long fromPositionExclusive, int maxCount, bool prefetch, ReadNextAllPage readNext, CancellationToken cancellationToken)
        {
            var position = fromPositionExclusive;
            maxCount = maxCount == int.MaxValue ? maxCount - 1 : maxCount;

            var sql = @"SELECT TOP(@count)
            streams.id_original As stream_id,
            messages.stream_version,
            messages.position,
            messages.id,
            messages.createdUtc,
            messages.type,
            messages.json_metadata,
            CASE WHEN @includeJsonData = true THEN messages.json_data ELSE null END
       FROM messages
 INNER JOIN streams
         ON messages.stream_id_internal = streams.id_internal
      WHERE messages.position >= @position
   ORDER BY messages.position;";

            using (var connection = await OpenConnection(cancellationToken))
            {
                using (var command = new SQLiteCommand(sql, connection))
                {
                    command.Parameters.AddWithValue("@position", position);
                    command.Parameters.AddWithValue("@count", maxCount + 1);
                    command.Parameters.AddWithValue("@includeJsonData", prefetch);
                    var reader = await command
                        .ExecuteReaderAsync(CommandBehavior.SequentialAccess, cancellationToken)
                        .NotOnCapturedContext();
                    
                    var messages = new List<StreamMessage>();
                    if (!reader.HasRows)
                    {
                        return new ReadAllPage(
                            fromPositionExclusive,
                            fromPositionExclusive,
                            true,
                            ReadDirection.Forward,
                            readNext,
                            messages.ToArray());
                    }

                    while (await reader.ReadAsync(cancellationToken).NotOnCapturedContext())
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
                                var jsonData = await reader.GetTextReader(7).ReadToEndAsync();
                                getJsonData = (ct) => Task.FromResult(jsonData);
                            }
                            else
                            {
                                var streamIdInfo = new StreamIdInfo(streamId);
                                getJsonData = ct => GetJsonData(streamIdInfo.SQLiteStreamId.Id, streamVersion, ct);
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

                    return new ReadAllPage(
                        fromPositionExclusive,
                        nextPosition,
                        isEnd,
                        ReadDirection.Forward,
                        readNext,
                        messages.ToArray());
                }
            }
        }

        protected override async Task<ReadAllPage> ReadAllBackwardsInternal(
            long fromPositionExclusive, 
            int maxCount, 
            bool prefetch, 
            ReadNextAllPage readNext, 
            CancellationToken cancellationToken)
        {
            maxCount = maxCount == int.MaxValue ? maxCount - 1 : maxCount;
            long position = fromPositionExclusive == Position.End ? long.MaxValue : fromPositionExclusive;

            using (var connection = await OpenConnection(cancellationToken))
            {
var sql = @"SELECT TOP(@count)
            streams.id_original As StreamId,
            messages.stream_version,
            messages.position,
            messages.id,
            messages.created_utc,
            messages.type,
            messages.json_metadata,
            CASE WHEN @includeJsonData = true THEN messages.json_data ELSE null END
       FROM messages
 INNER JOIN streams
         ON messages.stream_id_internal = streams.id_internal
      WHERE messages.position <= @position
   ORDER BY messages.position DESC;";
                using (var command = new SQLiteCommand(sql, connection))
                {
                    command.Parameters.AddWithValue("@count", maxCount);
                    command.Parameters.AddWithValue("@position", position);
                    command.Parameters.AddWithValue("@includeJsonData", prefetch);
                    var reader = await command
                        .ExecuteReaderAsync(CommandBehavior.SequentialAccess, cancellationToken)
                        .NotOnCapturedContext();

                    var messages = new List<StreamMessage>();
                    if (!reader.HasRows)
                    {
                        // When reading backwards and there are no more items, then next position is LongPosition.Start,
                        // regardless of what the fromPosition is.
                        return new ReadAllPage(
                            Position.Start,
                            Position.Start,
                            true,
                            ReadDirection.Backward,
                            readNext,
                            messages.ToArray());
                    }

                    long lastPosition = 0;
                    while (await reader.ReadAsync(cancellationToken).NotOnCapturedContext())
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
                            var jsonData = await reader.GetTextReader(7).ReadToEndAsync();
                            getJsonData = _ => Task.FromResult(jsonData);
                        }
                        else
                        {
                            var streamIdInfo = new StreamIdInfo(streamId);
                            getJsonData = ct => GetJsonData(streamIdInfo.SQLiteStreamId.Id, streamVersion, ct);
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

                    return new ReadAllPage(
                        fromPositionExclusive,
                        nextPosition,
                        isEnd,
                        ReadDirection.Backward,
                        readNext,
                        messages.ToArray());
                }
            }
        }
    }

    internal static class SqlDataReaderExtensions
    {
        internal static int? GetNullableInt32(this SQLiteDataReader reader, int ordinal) 
            => reader.IsDBNull(ordinal) 
                ? (int?) null 
                : reader.GetInt32(ordinal);

        internal static int? GetNullableInt32(this DbDataReader reader, int ordinal) 
            => reader.IsDBNull(ordinal) 
                ? (int?) null 
                : reader.GetInt32(ordinal);
    }
}