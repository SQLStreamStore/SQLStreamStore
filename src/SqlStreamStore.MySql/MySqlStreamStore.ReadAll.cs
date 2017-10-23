namespace SqlStreamStore
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Threading;
    using System.Threading.Tasks;
    using MySql.Data.MySqlClient;
    using SqlStreamStore.Streams;
    using SqlStreamStore.Infrastructure;

    public partial class MySqlStreamStore
    {
        protected override async Task<ReadAllPage> ReadAllForwardsInternal(
            long fromPositionExclusive,
            int maxCount,
            bool prefetch,
            ReadNextAllPage readNext,
            CancellationToken cancellationToken)
        {
            maxCount = maxCount == int.MaxValue ? maxCount - 1 : maxCount;
            var ordinal = MySqlOrdinal.CreateFromStreamStorePosition(fromPositionExclusive);

            using (var connection = _createConnection())
            {
                await connection.OpenAsync(cancellationToken).NotOnCapturedContext();

                var commandText = prefetch ? _scripts.ReadAllForwardWithData : _scripts.ReadAllForward;
                using (var command = new MySqlCommand(commandText, connection))
                {

                    command.Parameters.AddWithValue("ordinal", ordinal.ToMySqlOrdinal());
                    command.Parameters.AddWithValue("count", maxCount + 1); //Read extra row to see if at end or not

                    var messages = new List<StreamMessage>();

                    using(var reader = await command.ExecuteReaderAsync(cancellationToken)
                        .NotOnCapturedContext())
                    {
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
                            if(messages.Count == maxCount)
                            {
                                messages.Add(default(StreamMessage));
                            }
                            else
                            {
                                var streamId = reader.GetString(0);
                                var streamVersion = reader.GetInt32(1);
                                ordinal = MySqlOrdinal.CreateFromMySqlOrdinal(reader.GetInt64(2));
                                var eventId = reader.GetGuid(3);
                                var created = new DateTime(reader.GetInt64(4), DateTimeKind.Utc);
                                var type = reader.GetString(5);
                                var jsonMetadata = reader.GetString(6);

                                Func<CancellationToken, Task<string>> getJsonData;
                                if(prefetch)
                                {
                                    var jsonData = reader.GetString(7);
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
                                    ordinal.ToStreamStorePosition(),
                                    created,
                                    type,
                                    jsonMetadata,
                                    getJsonData);

                                messages.Add(message);
                            }
                        }
                    }

                    bool isEnd = true;

                    if (messages.Count == maxCount + 1) // An extra row was read, we're not at the end
                    {
                        isEnd = false;
                        messages.RemoveAt(maxCount);
                    }

                    var nextPosition = MySqlOrdinal.CreateFromMySqlOrdinal(messages[messages.Count - 1].Position + 1);

                    return new ReadAllPage(
                        fromPositionExclusive,
                        nextPosition.ToStreamStorePosition(),
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
            var ordinal = MySqlOrdinal.CreateFromStreamStorePosition(fromPositionExclusive);

            using (var connection = _createConnection())
            {
                await connection.OpenAsync(cancellationToken).NotOnCapturedContext();

                var commandText = prefetch ? _scripts.ReadAllBackwardWithData : _scripts.ReadAllBackward;
                using (var command = new MySqlCommand(commandText, connection))
                {
                    command.Parameters.AddWithValue("ordinal", ordinal.ToMySqlOrdinal());
                    command.Parameters.AddWithValue("count", maxCount + 1); //Read extra row to see if at end or not

                    var messages = new List<StreamMessage>();

                    long lastOrdinal;
                    using(var reader = await command.ExecuteReaderAsync(cancellationToken)
                        .NotOnCapturedContext())
                    {
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

                        lastOrdinal = 0;
                        while (await reader.ReadAsync(cancellationToken).NotOnCapturedContext())
                        {
                            var streamId = reader.GetString(0);
                            var streamVersion = reader.GetInt32(1);
                            ordinal = MySqlOrdinal.CreateFromMySqlOrdinal(reader.GetInt64(2));
                            var eventId = reader.GetGuid(3);
                            var created = new DateTime(reader.GetInt64(4), DateTimeKind.Utc);
                            var type = reader.GetString(5);
                            var jsonMetadata = reader.GetString(6);

                            Func<CancellationToken, Task<string>> getJsonData;
                            if (prefetch)
                            {
                                var jsonData = reader.GetString(7);
                                getJsonData = _ => Task.FromResult(jsonData);
                            }
                            else
                            {
                                var streamIdInfo = new StreamIdInfo(streamId);
                                getJsonData = ct => GetJsonData(streamIdInfo.SqlStreamId.Id, streamVersion, ct);
                            }

                            var message = new StreamMessage(
                                streamId,
                                eventId,
                                streamVersion,
                                ordinal.ToStreamStorePosition(),
                                created,
                                type,
                                jsonMetadata, getJsonData);

                            messages.Add(message);
                            lastOrdinal = ordinal.ToStreamStorePosition();
                        }
                    }

                    bool isEnd = true;
                    var nextPosition = lastOrdinal;

                    if (messages.Count == maxCount + 1) // An extra row was read, we're not at the end
                    {
                        isEnd = false;
                        messages.RemoveAt(maxCount);
                    }

                    fromPositionExclusive = messages.Any() 
                        ? messages[0].Position 
                        : 0;

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