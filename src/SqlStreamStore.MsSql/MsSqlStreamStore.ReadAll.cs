namespace SqlStreamStore
{
    using System;
    using System.Collections.Generic;
    using System.Data;
    using System.Linq;
    using System.Threading;
    using System.Threading.Tasks;
    using Microsoft.Data.SqlClient;
    using SqlStreamStore.Streams;

    public partial class MsSqlStreamStore
    {
        protected override async Task<ReadAllPage> ReadAllForwardsInternal(
            long fromPosition,
            int maxCount,
            bool prefetch,
            ReadNextAllPage<ReadAllPage> readNext,
            CancellationToken cancellationToken,
            long fromMaxPositionInclusive = -1)
        {
            maxCount = maxCount == int.MaxValue ? maxCount - 1 : maxCount;
            long position = fromPosition;

            using (var connection = _createConnection())
            {
                await connection.OpenAsync(cancellationToken).ConfigureAwait(false);

                var commandText = prefetch ? _scripts.ReadAllForwardWithData : _scripts.ReadAllForward;
                using (var command = new SqlCommand(commandText, connection))
                {
                    command.CommandTimeout = _commandTimeout;
                    command.Parameters.AddWithValue("position", position);
                    command.Parameters.AddWithValue("count", maxCount + 1); //Read extra row to see if at end or not
                    var reader = await command
                        .ExecuteReaderAsync(CommandBehavior.SequentialAccess, cancellationToken)
                        .ConfigureAwait(false);

                    List<StreamMessage> messages = new List<StreamMessage>();
                    if (!reader.HasRows)
                    {
                        return new ReadAllPage(
                            fromPosition,
                            fromPosition,
                            true,
                            ReadDirection.Forward,
                            readNext,
                            messages.ToArray());
                    }

                    while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
                    {
                        if(messages.Count == maxCount)
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
                            if(prefetch)
                            {
                                var jsonData = await reader.GetTextReader(7).ReadToEndAsync();
                                getJsonData = _ => Task.FromResult(jsonData);
                            }
                            else
                            {
                                var streamIdInfo = new StreamIdInfo(streamId);
                                getJsonData = ct => GetJsonData(streamIdInfo.SqlStreamId.Id, streamVersion, ct);
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

                    if (messages.Count == maxCount + 1) // An extra row was read, we're not at the end
                    {
                        isEnd = false;
                        messages.RemoveAt(maxCount);
                    }

                    var nextPosition = messages[messages.Count - 1].Position + 1;

                    return new ReadAllPage(
                        fromPosition,
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
            ReadNextAllPage<ReadAllPage> readNext,
            CancellationToken cancellationToken)
        {
            maxCount = maxCount == int.MaxValue ? maxCount - 1 : maxCount;
            long position = fromPositionExclusive == Position.End ? long.MaxValue : fromPositionExclusive;

            using (var connection = _createConnection())
            {
                await connection.OpenAsync(cancellationToken).ConfigureAwait(false);

                var commandText = prefetch ? _scripts.ReadAllBackwardWithData : _scripts.ReadAllBackward;
                using (var command = new SqlCommand(commandText, connection))
                {
                    command.CommandTimeout = _commandTimeout;
                    command.Parameters.AddWithValue("position", position);
                    command.Parameters.AddWithValue("count", maxCount + 1); //Read extra row to see if at end or not
                    var reader = await command
                        .ExecuteReaderAsync(CommandBehavior.SequentialAccess, cancellationToken)
                        .ConfigureAwait(false);

                    List<StreamMessage> messages = new List<StreamMessage>();
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
                    while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
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
                            getJsonData = ct => GetJsonData(streamIdInfo.SqlStreamId.Id, streamVersion, ct);
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
}
