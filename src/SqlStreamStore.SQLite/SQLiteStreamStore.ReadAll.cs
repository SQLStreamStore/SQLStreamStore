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
            long remainingMessages = -1;

            // query for empty store.
            using (var connection = OpenConnection())
            using(var command = connection.CreateCommand())
            {
                command.CommandText = @"SELECT COUNT(*) FROM messages; -- beginning of stream.";
                using(var reader = command.ExecuteReader())
                {
                    if((reader.Read() ? reader.GetInt64(0) : -1) < 0)
                    {
                        var result = new ReadAllPage(Position.Start, Position.Start, true, 
                            ReadDirection.Backward, readNext);
                        return Task.FromResult(result);
                    }
                }

                if(fromPositionExclusive == Position.End)
                {
                    command.CommandText = "SELECT MAX(position) FROM messages; -- last available position within allStream.";
                    command.Parameters.Clear();
                    using(var reader = command.ExecuteReader())
                    {
                        fromPositionExclusive = reader.Read() ? reader.GetInt64(0) : -1;
                    }
                }

                command.CommandText = @"SELECT coalesce(COUNT(*), -1) 
                                        FROM messages 
                                        WHERE position <= @position ORDER BY position DESC; -- determination of how many entries are left.";
                command.Parameters.Clear();
                command.Parameters.AddWithValue("@position", fromPositionExclusive);
                using(var reader = command.ExecuteReader())
                {
                    // is beginning of stream?
                    remainingMessages = reader.Read() ? reader.GetInt64(0) : -1;
                }

                if(remainingMessages <= 0)
                {
                    var result = new ReadAllPage(fromPositionExclusive, fromPositionExclusive, true,
                        ReadDirection.Backward, readNext);
                    return Task.FromResult(result);
                }
                
                command.CommandText = @"SELECT streams.id_original As StreamId,
                messages.stream_version,
                messages.position - 1,
                messages.message_id,
                messages.created_utc,
                messages.type,
                messages.json_metadata,
                CASE WHEN @includeJsonData THEN messages.json_data ELSE null END
           FROM messages
     INNER JOIN streams
             ON messages.stream_id_internal = streams.id_internal
          WHERE messages.position <= @position
       ORDER BY messages.position DESC
          LIMIT @count;"; 
                command.Parameters.Clear();
                command.Parameters.AddWithValue("@count", maxCount);
                command.Parameters.AddWithValue("@position", fromPositionExclusive);
                command.Parameters.AddWithValue("@includeJsonData", prefetch);
                
                var messages = new List<StreamMessage>();
                using(var reader = command.ExecuteReader())
                {
                    while(reader.Read())
                    {
                        var recordPositionId = reader.GetInt64(2);

                        var streamId = reader.GetString(0);
                        var streamVersion = reader.GetInt32(1);
                        var messageId = reader.GetGuid(3);
                        var created = reader.GetDateTime(4);
                        var type = reader.GetString(5);
                        var jsonMetadata = reader.GetString(6);

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
                                Task.FromResult(GetJsonData(command,
                                    streamIdInfo.SQLiteStreamId.Id,
                                    streamVersion));
                        }

                        var message = new StreamMessage(
                            streamId,
                            messageId,
                            streamVersion,
                            recordPositionId,
                            created,
                            type,
                            jsonMetadata,
                            getJsonData);

                        messages.Add(message);
                    }
                }

                bool isEnd = remainingMessages -  messages.Count <= 0;

                var lastMessage = messages.Last();
                var nextPosition = isEnd ? Position.Start : lastMessage.Position; 
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

    // internal static class SqlDataReaderExtensions
    // {
    //     internal static int? GetNullableInt32(this SqliteDataReader reader, int ordinal) 
    //         => reader.IsDBNull(ordinal) 
    //             ? (int?) null 
    //             : reader.GetInt32(ordinal);
    //
    //     internal static int? GetNullableInt32(this DbDataReader reader, int ordinal) 
    //         => reader.IsDBNull(ordinal) 
    //             ? (int?) null 
    //             : reader.GetInt32(ordinal);
    // }
}