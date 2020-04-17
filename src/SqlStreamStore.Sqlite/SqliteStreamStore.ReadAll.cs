namespace SqlStreamStore
{
    using System;
    using System.Collections.Generic;
    using System.Data;
    using System.Linq;
    using System.Threading;
    using System.Threading.Tasks;
    using SqlStreamStore.Streams;

    public partial class SqliteStreamStore
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

            using (var connection = OpenConnection())
            using (var command = connection.CreateCommand())
            {
                // find starting node.
                command.CommandText = @"SELECT MAX(messages.[position]) FROM messages;";
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

                if(allStreamPosition < fromPositionExclusive)
                {
                    return Task.FromResult(new ReadAllPage(
                        fromPositionExclusive, 
                        fromPositionExclusive, 
                        true, 
                        ReadDirection.Forward, 
                        readNext));
                }
                
                // determine number of remaining messages.
                command.CommandText = @"SELECT COUNT(*) FROM messages WHERE messages.[position] >= @position";
                command.Parameters.Clear();
                command.Parameters.AddWithValue("@position", fromPositionExclusive);
                var remainingMessages = command.ExecuteScalar(Position.End);
                if(remainingMessages == Position.End)
                {
                    return Task.FromResult(new ReadAllPage(
                        fromPositionExclusive,
                        Position.End,
                        true,
                        ReadDirection.Forward,
                        readNext));
                }
                
                
                var messages = new List<StreamMessage>();
                command.CommandText = @"SELECT streams.id_original As stream_id,
        messages.stream_version,
        messages.position,
        messages.event_id,
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
                command.Parameters.AddWithValue("@position", fromPositionExclusive);
                command.Parameters.AddWithValue("@count", maxCount);
                command.Parameters.AddWithValue("@includeJsonData", prefetch);
                var reader = command.ExecuteReader(CommandBehavior.SequentialAccess);

                var _continue = true;
                while (reader.Read() && _continue)
                {
                    if(messages.Count == maxCount)
                    {
                        _continue = false;
                        continue;
                    }

                    var streamId = reader.GetString(0);
                    var streamVersion = reader.GetInt32(1);
                    var position = reader.IsDBNull(2) ? Position.End : reader.GetInt64(2);
                    var messageId = reader.GetGuid(3);
                    var created = reader.GetDateTime(4);
                    var type = reader.GetString(5);
                    var jsonMetadata = reader.GetString(6);
                    var preloadJson = (!reader.IsDBNull(7) && prefetch)
                        ? reader.GetTextReader(7).ReadToEnd()
                        : default;

                    var message = new StreamMessage(streamId,
                        messageId,
                        streamVersion,
                        position,
                        created,
                        type,
                        jsonMetadata,
                        ct => prefetch
                            ? Task.FromResult(preloadJson)
                            : SqliteCommandExtensions.GetJsonData(streamId, streamVersion));

                    messages.Add(message);
                }

                bool isEnd = remainingMessages - messages.Count <= 0;
                var nextPosition = messages.Any() 
                    ? messages.Last().Position + 1
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

        protected override async Task<ReadAllPage> ReadAllBackwardsInternal(
            long fromPosition,
            int maxCount,
            bool prefetch,
            ReadNextAllPage readNext,
            CancellationToken cancellationToken)
        {
            GuardAgainstDisposed();
            cancellationToken.ThrowIfCancellationRequested();

            using (var connection = OpenConnection())
            using (var command = connection.CreateCommand())
            {
                long? beginningPosition = fromPosition;
                var allStreamPosition = await connection.AllStream()
                    .ReadHeadPosition(cancellationToken);
                if(allStreamPosition == Position.None)
                {
                    return new ReadAllPage(
                        Position.Start,
                        Position.Start,
                        true,
                        ReadDirection.Backward,
                        readNext);
                }

                if(fromPosition == Position.End)
                {
                    beginningPosition = allStreamPosition > fromPosition ? allStreamPosition : fromPosition;
                }

                if(fromPosition > allStreamPosition && fromPosition > Position.Start)
                {
                    return new ReadAllPage(
                        fromPosition, 
                        fromPosition, 
                        true, 
                        ReadDirection.Backward, 
                        readNext);
                }
                
                // determine number of remaining messages.
                command.CommandText = @"SELECT COUNT(*) FROM messages WHERE messages.[position] <= @position";
                command.Parameters.Clear();
                command.Parameters.AddWithValue("@position", beginningPosition);
                var remainingMessages = command.ExecuteScalar(Position.End);
                if(remainingMessages == Position.End)
                {
                    return new ReadAllPage(
                        allStreamPosition ?? Position.Start,
                        Position.End,
                        true,
                        ReadDirection.Backward,
                        readNext);
                }
                command.CommandText = @"SELECT streams.id_original As stream_id,
                                                messages.stream_version,
                                                messages.position,
                                                messages.event_id,
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
                command.Parameters.Clear();
                command.Parameters.AddWithValue("@position", beginningPosition);
                command.Parameters.AddWithValue("@count", maxCount);
                command.Parameters.AddWithValue("@includeJsonData", prefetch);

                var messages = new List<StreamMessage>();
                using(var reader = command.ExecuteReader())
                {
                    while(reader.Read())
                    {
                        var streamId = reader.GetString(0);
                        var streamVersion = reader.GetInt32(1);
                        var position = reader.IsDBNull(2) ? Position.End : reader.GetInt64(2);
                        var eventId = reader.GetGuid(3);
                        var created = reader.GetDateTime(4);
                        var type = reader.GetString(5);
                        var jsonMetadata = reader.GetString(6);
                        var preloadJson = (!reader.IsDBNull(7) && prefetch)
                            ? reader.GetTextReader(7).ReadToEnd()
                            : default;

                        var message = new StreamMessage(streamId,
                            eventId,
                            streamVersion,
                            position,
                            created,
                            type,
                            jsonMetadata,
                            ct => prefetch
                                ? Task.FromResult(preloadJson)
                                : SqliteCommandExtensions.GetJsonData(streamId, streamVersion));

                        messages.Add(message);
                    }

                    bool isEnd = remainingMessages - messages.Count <= 0;
                
                    var nextPosition = messages.Any() ? Math.Max(messages.Last().Position - 1, Position.Start) : Position.Start;

                    return new ReadAllPage(
                        beginningPosition.Value,
                        nextPosition,
                        isEnd,
                        ReadDirection.Backward,
                        readNext,
                        messages.ToArray());
                }
            }
        }
    }
}