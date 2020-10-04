namespace SqlStreamStore
{
    using System;
    using System.Collections.Generic;
    using System.Data;
    using System.Data.Common;
    using System.Threading;
    using System.Threading.Tasks;
    using MySqlConnector;
    using SqlStreamStore.Infrastructure;
    using SqlStreamStore.MySqlScripts;
    using SqlStreamStore.Streams;

    partial class MySqlStreamStore
    {
        protected override async Task<ReadAllPage> ReadAllForwardsInternal(
            long fromPositionExclusive,
            int maxCount,
            bool prefetch,
            ReadNextAllPage readNext,
            CancellationToken cancellationToken)
        {
            maxCount = maxCount == int.MaxValue ? maxCount - 1 : maxCount;

            try
            {
                var commandText = prefetch
                    ? _schema.ReadAllForwardsWithData
                    : _schema.ReadAllForwards;

                using (var connection = await OpenConnection(cancellationToken))
                using (var transaction = await connection.BeginTransactionAsync(cancellationToken).ConfigureAwait(false))
                using (var command = BuildStoredProcedureCall(
                    commandText,
                    transaction,
                    Parameters.Count(maxCount + 1),
                    Parameters.Position(fromPositionExclusive)))

                using (var reader = await command
                            .ExecuteReaderAsync(CommandBehavior.SequentialAccess, cancellationToken)
                            .ConfigureAwait(false))
                {
                    if (!reader.HasRows)
                    {
                        return new ReadAllPage(
                            fromPositionExclusive,
                            fromPositionExclusive,
                            true,
                            ReadDirection.Forward,
                            readNext,
                            Array.Empty<StreamMessage>());
                    }

                    var messages = new List<(StreamMessage message, int? maxAge)>();

                    while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
                    {
                        if (messages.Count == maxCount)
                        {
                            messages.Add(default);
                        }
                        else
                        {
                            var streamIdInfo = new StreamIdInfo(reader.GetString(0));
                            var (message, maxAge, _) =
                                await ReadAllStreamMessage(reader, streamIdInfo.MySqlStreamId, prefetch);
                            messages.Add((message, maxAge));
                        }
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
                        fromPositionExclusive,
                        nextPosition,
                        isEnd,
                        ReadDirection.Forward,
                        readNext,
                        filteredMessages.ToArray());
                }
            }
            catch(MySqlException exception) when(exception.InnerException is ObjectDisposedException disposedException)
            {
                throw new ObjectDisposedException(disposedException.Message, exception);
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
            var ordinal = fromPositionExclusive == Position.End ? long.MaxValue - 1 : fromPositionExclusive;

            try
            {
                var commandText = prefetch
                    ? _schema.ReadAllBackwardsWithData
                    : _schema.ReadAllBackwards;

                using (var connection = await OpenConnection(cancellationToken))
                using (var transaction = await connection.BeginTransactionAsync(cancellationToken).ConfigureAwait(false))
                using (var command = BuildStoredProcedureCall(
                    commandText,
                    transaction,
                    Parameters.Count(maxCount + 1),
                    Parameters.Position(ordinal)))
                using (var reader = await command
                    .ExecuteReaderAsync(CommandBehavior.SequentialAccess, cancellationToken)
                    .ConfigureAwait(false))
                {
                    if(!reader.HasRows)
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

                    var messages = new List<(StreamMessage message, int? maxAge)>();

                    long lastOrdinal = 0;
                    while(await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
                    {
                        var streamIdInfo = new StreamIdInfo(reader.GetString(0));
                        var (message, maxAge, position) =
                            await ReadAllStreamMessage(reader, streamIdInfo.MySqlStreamId, prefetch);
                        messages.Add((message, maxAge));

                        lastOrdinal = position;
                    }

                    bool isEnd = true;
                    var nextPosition = lastOrdinal;

                    if(messages.Count == maxCount + 1) // An extra row was read, we're not at the end
                    {
                        isEnd = false;
                        messages.RemoveAt(maxCount);
                    }

                    var filteredMessages = FilterExpired(messages);

                    fromPositionExclusive = filteredMessages.Count > 0 ? filteredMessages[0].Position : 0;

                    return new ReadAllPage(
                        fromPositionExclusive,
                        nextPosition,
                        isEnd,
                        ReadDirection.Backward,
                        readNext,
                        filteredMessages.ToArray());
                }
            }
            catch(MySqlException exception) when(exception.InnerException is ObjectDisposedException disposedException)
            {
                throw new ObjectDisposedException(disposedException.Message, exception);
            }
        }

        private async Task<(StreamMessage message, int? maxAge, long position)> ReadAllStreamMessage(
            DbDataReader reader,
            MySqlStreamId streamId,
            bool prefetch)
        {
            async Task<string> ReadString(int ordinal)
            {
                if (await reader.IsDBNullAsync(ordinal))
                {
                    return null;
                }

                using (var textReader = reader.GetTextReader(ordinal))
                {
                    return await textReader.ReadToEndAsync().ConfigureAwait(false);
                }
            }

            var maxAge = await reader.IsDBNullAsync(1).ConfigureAwait(false)
                ? default
                : reader.GetInt32(1);
            var messageId = reader.GetGuid(2);
            var streamVersion = reader.GetInt32(3);
            var position = reader.GetInt64(4);
            var createdUtc = reader.GetDateTime(5);
            var type = reader.GetString(6);
            var jsonMetadata = await ReadString(7);

            StreamMessage streamMessage;

            if (prefetch)
            {

                var jsonData = await ReadString(8);

                streamMessage = new StreamMessage(
                    streamId.IdOriginal,
                    messageId,
                    streamVersion,
                    position,
                    createdUtc,
                    type,
                    jsonMetadata,
                    jsonData);
            }
            else
            {
                streamMessage = new StreamMessage(
                    streamId.IdOriginal,
                    messageId,
                    streamVersion,
                    position,
                    createdUtc,
                    type,
                    jsonMetadata,
                    ct => GetJsonData(streamId, streamVersion)(ct));
            }

            return (streamMessage, maxAge, position);
        }
    }
}
