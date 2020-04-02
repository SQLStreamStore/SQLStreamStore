namespace SqlStreamStore
{
    using System;
    using System.Collections.Generic;
    using System.Data;
    using System.Data.SQLite;
    using System.Linq;
    using System.Threading;
    using System.Threading.Tasks;
    using SqlStreamStore.Infrastructure;
    using SqlStreamStore.Streams;

    public partial class SQLiteStreamStore
    {
        protected override async Task<ReadStreamPage> ReadStreamForwardsInternal(
            string streamId, 
            int start, 
            int count, 
            bool prefetch, 
            ReadNextStreamPage readNext, 
            CancellationToken cancellationToken)
        {
            using (var connection = await OpenConnection(cancellationToken))
            {
                var streamIdInfo = new StreamIdInfo(streamId);
                return await ReadStreamInternal(
                    streamIdInfo.SQLiteStreamId,
                    start,
                    count,
                    ReadDirection.Forward,
                    prefetch,
                    readNext,
                    connection,
                    null,
                    cancellationToken);
            }
        }

        protected override async Task<ReadStreamPage> ReadStreamBackwardsInternal(string streamId, int fromVersionInclusive, int count, bool prefetch, ReadNextStreamPage readNext, CancellationToken cancellationToken)
        {
            using (var connection = await OpenConnection(cancellationToken))
            {
                var streamIdInfo = new StreamIdInfo(streamId);
                return await ReadStreamInternal(
                    streamIdInfo.SQLiteStreamId,
                    fromVersionInclusive,
                    count,
                    ReadDirection.Backward,
                    prefetch,
                    readNext,
                    connection,
                    null,
                    cancellationToken);
            }
        }
        private async Task<ReadStreamPage> ReadStreamInternal(
            SQLiteStreamId sqlStreamId,
            int start,
            int count,
            ReadDirection direction,
            bool prefetch,
            ReadNextStreamPage readNext,
            SQLiteConnection connection,
            SQLiteTransaction transaction,
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
      WHERE streams.Id = @streamId";
                command.Parameters.Add(new SQLiteParameter("@streamId", sqlStreamId.Id));

                var streamIdInternal = default(int?);
                var streamVersion = default(int?);
                var streamPosition = default(int?);

                using (var reader = await command.ExecuteReaderAsync(cancellationToken))
                {
                    if (reader.IsDBNull(0))
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

                    streamIdInternal = reader.GetNullableInt32(0);
                    streamVersion = reader.GetNullableInt32(1);
                    streamPosition = reader.GetNullableInt32(2);
                }

                if (streamPosition == null)
                {
                    command.CommandText = @"SELECT min(messages.position)
                    FROM messages
                    WHERE messages.stream_id_internal = @streamIdInternal
                        AND messages.stream_version >= @streamVersion";
                    command.Parameters.Clear();
                    command.Parameters.Add(new SQLiteParameter("@streamId", streamIdInternal));
                    command.Parameters.Add(new SQLiteParameter("@streamVersion", streamVersion));
                    streamPosition = (int?)await command.ExecuteScalarAsync(cancellationToken).NotOnCapturedContext();
                }

                command.CommandText = @"SELECT TOP(@count)
            messages.stream_version,
            messages.position,
            messages.id AS event_id,
            messages.createdUtc,
            messages.type,
            messages.json_metadata,
            CASE WHEN @includeJsonData = true THEN messages.json_data ELSE null END
       FROM messages
      WHERE messages.StreamIdInternal = @streamIdInternal AND dbo.Messages.Position >= @position
   ORDER BY messages.Position;";
                command.Parameters.Clear();
                command.Parameters.Add(new SQLiteParameter("@streamIdInternal", streamIdInternal));
                command.Parameters.Add(new SQLiteParameter("@count", count + 1 ));
                command.Parameters.Add(new SQLiteParameter("@position", streamPosition));
                command.Parameters.Add(new SQLiteParameter("@includeJsonData", prefetch));

                using (var reader = await command.ExecuteReaderAsync(CommandBehavior.SequentialAccess, cancellationToken).NotOnCapturedContext())
                {
                    var messages = new List<StreamMessage>();
                    while (await reader.ReadAsync(cancellationToken).NotOnCapturedContext())
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
                                var jsonData = await reader.GetTextReader(6).ReadToEndAsync();
                                getJsonData = _ => Task.FromResult(jsonData);
                            }
                            else
                            {
                                getJsonData = ct => GetJsonData(sqlStreamId.Id, streamVersion1, ct);
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
                        getNextVersion(messages, streamVersion.Value),
                        streamVersion.Value,
                        streamPosition.Value,
                        direction,
                        isEnd,
                        readNext,
                        messages.ToArray());
                }
            }
        }

        private async Task<string> GetJsonData(string streamId, int streamVersion, CancellationToken cancellationToken)
        {
            using(var connection = await OpenConnection(cancellationToken))
            {
                var sql = @"SELECT messages.json_data
FROM messages
WHERE messages.stream_id_internal = 
  (
    SELECT streams.id_internal
    FROM streams
    WHERE streams.id = @streamId)
  AND messages.stream_version = @streamVersion";
                using(var command = new SQLiteCommand(sql, connection))
                {
                    command.Parameters.Add(new SQLiteParameter("@streamId", streamId));
                    command.Parameters.AddWithValue("@streamVersion", streamVersion);

                    using(var reader = await command
                        .ExecuteReaderAsync(
                            CommandBehavior.SequentialAccess | CommandBehavior.SingleRow, 
                            cancellationToken)
                        .NotOnCapturedContext())
                    {
                        if(await reader.ReadAsync(cancellationToken).NotOnCapturedContext())
                        {
                            return await reader.GetTextReader(0).ReadToEndAsync();
                        }
                        return null;
                    }
                }
            }
        }
    }
}