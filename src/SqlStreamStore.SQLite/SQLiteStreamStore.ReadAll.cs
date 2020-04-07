namespace SqlStreamStore
{
    using System;
    using System.Collections.Generic;
    using System.Data;
    using System.Linq;
    using System.Threading;
    using System.Threading.Tasks;
    using SqlStreamStore.Streams;

    public partial class SQLiteStreamStore
    {
        protected override Task<ReadAllPage> ReadAllForwardsInternal(
            long fromPositionExclusive, 
            int maxCount, 
            bool prefetch, 
            ReadNextAllPage readNext, 
            CancellationToken cancellationToken)
        {
            GuardAgainstDisposed();
            cancellationToken.ThrowIfCancellationRequested();

            long remainingMessages = -1;

            using (var connection = OpenConnection())
            using (var command = connection.CreateCommand())
            {
                // find starting node.
                command.CommandText = @"SELECT MAX(position) FROM messages;";
                command.Parameters.Clear();
                var allStreamPosition = command.ExecuteScalar<long?>();
                if(allStreamPosition == Position.None)
                {
                    return Task.FromResult(new ReadAllPage(
                        Position.Start, 
                        Position.Start, 
                        true, 
                        ReadDirection.Forward, 
                        readNext));
                }

                // determine if requested position start is past end of store.
                command.CommandText = @"SELECT MIN(position) FROM messages WHERE position > @position";
                command.Parameters.Clear();
                command.Parameters.AddWithValue("@position", fromPositionExclusive);
                var pastPositionExclusive = command.ExecuteScalar<long?>();
                if(pastPositionExclusive == Position.None)
                {
                    return Task.FromResult(new ReadAllPage(
                        fromPositionExclusive, 
                        fromPositionExclusive, 
                        true, 
                        ReadDirection.Forward, 
                        readNext));
                }
                
                // determine number of remaining messages.
                command.CommandText = @"SELECT COUNT(*) FROM messages WHERE position >= @position";
                command.Parameters.Clear();
                command.Parameters.AddWithValue("@position", fromPositionExclusive);
                remainingMessages  = command.ExecuteScalar<long?>() ?? Position.End;
                if(remainingMessages == Position.End)
                {
                    return Task.FromResult(new ReadAllPage(
                        fromPositionExclusive, 
                        fromPositionExclusive, 
                        true, 
                        ReadDirection.Forward, 
                        readNext));
                }
                
                
                var messages = new List<StreamMessage>();
                command.CommandText = @"SELECT streams.id_original As stream_id,
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
  WHERE messages.position >= @position
ORDER BY messages.position
  LIMIT @count;";
                command.Parameters.Clear();
                command.Parameters.AddWithValue("@position", pastPositionExclusive);
                command.Parameters.AddWithValue("@count", maxCount);
                command.Parameters.AddWithValue("@includeJsonData", prefetch);
                var reader = command.ExecuteReader(CommandBehavior.SequentialAccess);

                while (reader.Read())
                {
                    pastPositionExclusive = reader.GetInt64(2);

                    var streamId = reader.GetString(0);
                    var streamVersion = reader.GetInt32(1);
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
                        getJsonData = ct => Task.FromResult(GetJsonData(streamIdInfo.SQLiteStreamId.Id, streamVersion));
                    }

                    var message = new StreamMessage(streamId,
                        messageId,
                        streamVersion,
                        pastPositionExclusive ?? Position.End,
                        created,
                        type,
                        jsonMetadata,
                        getJsonData);

                    messages.Add(message);
                }

                bool isEnd = remainingMessages - messages.Count <= 0;
                var nextPosition = messages.Any() 
                    ? messages.Last().Position 
                    : Position.End;

                return Task.FromResult(new ReadAllPage(
                    fromPositionExclusive,
                    nextPosition,
                    isEnd,
                    ReadDirection.Forward,
                    readNext,
                    messages.ToArray()));
            }
        }

        protected override Task<ReadAllPage> ReadAllBackwardsInternal(
            long fromPositionExclusive, 
            int maxCount, 
            bool prefetch, 
            ReadNextAllPage readNext, 
            CancellationToken cancellationToken)
        {
            GuardAgainstDisposed();
            cancellationToken.ThrowIfCancellationRequested();

            maxCount = maxCount == int.MaxValue ? maxCount - 1 : maxCount;
            long position = fromPositionExclusive == Position.End ? long.MaxValue - 1: fromPositionExclusive;

            // query for empty store.
            using (var connection = OpenConnection())
            using(var command = connection.CreateCommand())
            {
                command.CommandText = @"SELECT COUNT(*) 
                                        FROM messages 
                                        WHERE position < @position ORDER BY position DESC; -- determination of how many entries are left.";
                command.Parameters.Clear();
                command.Parameters.AddWithValue("@position", position);
                var remainingMessages = command.ExecuteScalar<long?>();

                if(remainingMessages == null)
                {
                    var result = new ReadAllPage(
                        Position.Start, 
                        Position.Start, 
                        true, 
                        ReadDirection.Backward, 
                        readNext,
                        new StreamMessage[0]);
                    return Task.FromResult(result);
                }

                command.CommandText = @"SELECT streams.id_original As StreamId,
                messages.stream_version,
                messages.position - 1,
                messages.message_id,
                messages.created_utc,
                messages.type,
                messages.json_metadata,
                CASE WHEN @includeJsonData THEN messages.json_data ELSE null END,
                streams.max_age
           FROM messages
     INNER JOIN streams
             ON messages.stream_id_internal = streams.id_internal
          WHERE messages.position - 1 <= @position
       ORDER BY messages.position DESC
          LIMIT @count;"; 
                command.Parameters.Clear();
                command.Parameters.AddWithValue("@count", maxCount + 1);
                command.Parameters.AddWithValue("@position", position);
                command.Parameters.AddWithValue("@includeJsonData", prefetch);

                long lastPosition = 0;
                var messages = new List<(StreamMessage Message, int? MaxAge)>();
                using(var reader = command.ExecuteReader())
                {
                    while(reader.Read())
                    {
                        if(messages.Count == maxCount)
                        {
                            position = reader.GetInt64(2);
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
                            var maxAge = reader.IsDBNull(8) 
                                ? default(int?) 
                                : reader.GetInt32(8);

                            Func<CancellationToken, Task<string>> getJsonData;
                            if(prefetch)
                            {
                                var jsonData = reader.GetTextReader(7).ReadToEnd();
                                getJsonData = _ => Task.FromResult(jsonData);
                            }
                            else
                            {
                                var streamIdInfo = new StreamIdInfo(streamId);
                                getJsonData = ct =>
                                    Task.FromResult(GetJsonData(streamIdInfo.SQLiteStreamId.Id, streamVersion));
                            }

                            var message = new StreamMessage(
                                streamId,
                                messageId,
                                streamVersion,
                                position,
                                created,
                                type,
                                jsonMetadata,
                                getJsonData);

                            messages.Add((message, maxAge));
                        }

                        lastPosition = position;
                    }
                }

                var filtered = FilterExpired(messages);

                bool isEnd = remainingMessages - filtered.Count <= 0;
                var nextPosition = lastPosition;
                fromPositionExclusive = filtered.Any() ? filtered.First().Position : 0;

                return Task.FromResult(new ReadAllPage(
                    fromPositionExclusive,
                    nextPosition,
                    isEnd,
                    ReadDirection.Backward,
                    readNext,
                    filtered.ToArray()));
            }
        }
    }
}