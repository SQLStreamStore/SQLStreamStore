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
    using SqlStreamStore.Infrastructure;

    public partial class MsSqlStreamStoreV3
    {
        protected override async Task<ReadAllPage> ReadAllForwardsInternal(
            long fromPosition,
            int maxCount,
            bool prefetch,
            ReadNextAllPage readNext,
            CancellationToken cancellationToken)
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

                    if (!reader.HasRows)
                    {
                        return new ReadAllPage(
                            fromPosition,
                            fromPosition,
                            true,
                            ReadDirection.Forward,
                            readNext,
                            Array.Empty<StreamMessage>());
                    }

                    var messages = new List<(StreamMessage, int?)>();

                    while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
                    {
                        var ordinal = 0;
                        var streamId = reader.GetString(ordinal++);
                        var maxAge = reader.GetNullableInt32(ordinal++);
                        var streamVersion = reader.GetInt32(ordinal++);
                        position = reader.GetInt64(ordinal++);
                        var eventId = reader.GetGuid(ordinal++);
                        var created = reader.GetDateTime(ordinal++);
                        var type = reader.GetString(ordinal++);
                        var jsonMetadata = reader.GetString(ordinal++);

                        Func<CancellationToken, Task<string>> getJsonData;
                        if(prefetch)
                        {
                            var jsonData = await reader.GetTextReader(ordinal).ReadToEndAsync();
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

                    bool isEnd = true;

                    if (messages.Count == maxCount + 1) // An extra row was read, we're not at the end
                    {
                        isEnd = false;
                        messages.RemoveAt(maxCount);
                    }

                    var filteredMessages = FilterExpired(messages);

                    var nextPosition = filteredMessages[filteredMessages.Count - 1].Position + 1;

                    return new ReadAllPage(
                        fromPosition,
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

                    var messages = new List<(StreamMessage, int?)>();
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
                            Array.Empty<StreamMessage>());
                    }

                    long lastPosition = 0;
                    while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
                    {
                        var ordinal = 0;
                        var streamId = reader.GetString(ordinal++);
                        var maxAge = reader.GetNullableInt32(ordinal++);
                        var streamVersion = reader.GetInt32(ordinal++);
                        position = reader.GetInt64(ordinal++);
                        var messageId = reader.GetGuid(ordinal++);
                        var created = reader.GetDateTime(ordinal++);
                        var type = reader.GetString(ordinal++);
                        var jsonMetadata = reader.GetString(ordinal++);

                        Func<CancellationToken, Task<string>> getJsonData;
                        if (prefetch)
                        {
                            var jsonData = await reader.GetTextReader(ordinal).ReadToEndAsync();
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

                        messages.Add((message, maxAge));
                        lastPosition = position;
                    }


                    bool isEnd = true;
                    var nextPosition = lastPosition;

                    if (messages.Count == maxCount + 1) // An extra row was read, we're not at the end
                    {
                        isEnd = false;
                        messages.RemoveAt(maxCount);
                    }

                    var filteredMessages = FilterExpired(messages);

                    fromPositionExclusive = filteredMessages.Any() ? filteredMessages[0].Position : 0;

                    return new ReadAllPage(
                        fromPositionExclusive,
                        nextPosition,
                        isEnd,
                        ReadDirection.Backward,
                        readNext,
                        filteredMessages.ToArray());
                }
            }
        }
    }

    internal static class SqlDataReaderExtensions
    {
        internal static int? GetNullableInt32(this SqlDataReader reader, int ordinal)
            => reader.IsDBNull(ordinal)
                ? (int?) null
                : reader.GetInt32(ordinal);
    }
}
