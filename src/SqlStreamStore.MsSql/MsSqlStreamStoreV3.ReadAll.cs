namespace SqlStreamStore
{
    using System;
    using System.Collections.Generic;
    using System.Data.SqlClient;
    using System.Linq;
    using System.Threading;
    using System.Threading.Tasks;
    using SqlStreamStore.Streams;
    using SqlStreamStore.Infrastructure;

    public partial class MsSqlStreamStoreV3
    {
        protected override async Task<ReadAllPage> ReadAllForwardsInternal(
            long fromPositionExlusive,
            int maxCount,
            bool prefetch,
            ReadNextAllPage readNext,
            CancellationToken cancellationToken)
        {
            maxCount = maxCount == int.MaxValue ? maxCount - 1 : maxCount;
            long position = fromPositionExlusive;

            using (var connection = _createConnection())
            {
                await connection.OpenAsync(cancellationToken).NotOnCapturedContext();

                var commandText = prefetch ? _scripts.ReadAllForwardWithData : _scripts.ReadAllForward;
                using (var command = new SqlCommand(commandText, connection))
                {
                    command.Parameters.AddWithValue("position", position);
                    command.Parameters.AddWithValue("count", maxCount + 1); //Read extra row to see if at end or not
                    var reader = await command
                        .ExecuteReaderAsync(cancellationToken)
                        .NotOnCapturedContext();

                    List<(StreamMessage, int)> messages = new List<(StreamMessage, int)>();
                    if (!reader.HasRows)
                    {
                        return new ReadAllPage(
                            fromPositionExlusive,
                            fromPositionExlusive,
                            true,
                            ReadDirection.Forward,
                            readNext,
                            Array.Empty<StreamMessage>());
                    }

                    while (await reader.ReadAsync(cancellationToken).NotOnCapturedContext())
                    {
                        if(messages.Count == maxCount)
                        {
                            messages.Add((default(StreamMessage), 0));
                        }
                        else
                        {
                            var streamId = reader.GetString(0);
                            var maxAge = reader.GetInt32(1);
                            var streamVersion = reader.GetInt32(2);
                            position = reader.GetInt64(3);
                            var eventId = reader.GetGuid(4);
                            var created = reader.GetDateTime(5);
                            var type = reader.GetString(6);
                            var jsonMetadata = reader.GetString(7);

                            Func<CancellationToken, Task<string>> getJsonData;
                            if(prefetch)
                            {
                                var jsonData = reader.GetString(8);
                                getJsonData = _ => Task.FromResult(jsonData);
                            }
                            else
                            {
                                var streamIdInfo = new StreamIdInfo(streamId);
                                getJsonData = ct => GetJsonData(streamIdInfo.SqlStreamId.Id, streamVersion, ct);
                            }

                            var message = new StreamMessage(streamId,
                                eventId,
                                streamVersion,
                                position,
                                created,
                                type,
                                jsonMetadata,
                                getJsonData);

                            messages.Add((message, maxAge));
                        }
                    }

                    var filteredMessages = FilterExpired(messages);

                    bool isEnd = true;

                    if (messages.Count == maxCount + 1) // An extra row was read, we're not at the end
                    {
                        isEnd = false;
                        messages.RemoveAt(maxCount);
                    }

                    var nextPosition = filteredMessages[messages.Count - 1].Position + 1;

                    return new ReadAllPage(
                        fromPositionExlusive,
                        nextPosition,
                        isEnd,
                        ReadDirection.Forward,
                        readNext,
                        filteredMessages.ToArray());
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

            using (var connection = _createConnection())
            {
                await connection.OpenAsync(cancellationToken).NotOnCapturedContext();

                var commandText = prefetch ? _scripts.ReadAllBackwardWithData : _scripts.ReadAllBackward;
                using (var command = new SqlCommand(commandText, connection))
                {
                    command.Parameters.AddWithValue("position", position);
                    command.Parameters.AddWithValue("count", maxCount + 1); //Read extra row to see if at end or not
                    var reader = await command
                        .ExecuteReaderAsync(cancellationToken)
                        .NotOnCapturedContext();

                    List<StreamMessage> messages = new List<StreamMessage>();
                    if (!reader.HasRows)
                    {
                        // When reading backwards and there are no more items, then next position is LongPosition.Start,
                        // regardles of what the fromPosition is.
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
                        var maxAge = reader.GetInt32(1);
                        var streamVersion = reader.GetInt32(2);
                        position = reader.GetInt64(3);
                        var messageId = reader.GetGuid(4);
                        var created = reader.GetDateTime(5);
                        var type = reader.GetString(6);
                        var jsonMetadata = reader.GetString(7);

                        Func<CancellationToken, Task<string>> getJsonData;
                        if (prefetch)
                        {
                            var jsonData = reader.GetString(8);
                            getJsonData = _ => Task.FromResult(jsonData);
                        }
                        else
                        {
                            var streamIdInfo = new StreamIdInfo(streamId);
                            getJsonData = ct => GetJsonData(streamIdInfo.SqlStreamId.Id, streamVersion, ct);
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
}