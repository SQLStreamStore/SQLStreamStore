namespace SqlStreamStore
{
    using System;
    using System.Collections.Generic;
    using System.Threading;
    using System.Threading.Tasks;
    using SqlStreamStore.Infrastructure;
    using SqlStreamStore.PgSqlScripts;
    using SqlStreamStore.Streams;

    partial class PostgresStreamStore
    {
        protected override async Task<ReadAllPage> ReadAllForwardsInternal(
            long fromPositionExclusive,
            int maxCount,
            bool prefetch,
            ReadNextAllPage readNext,
            CancellationToken cancellationToken)
        {
            maxCount = maxCount == int.MaxValue ? maxCount - 1 : maxCount;
            var ordinal = fromPositionExclusive;

            using(var connection = _createConnection())
            {
                await connection.OpenAsync(cancellationToken).NotOnCapturedContext();
                using(var transaction = connection.BeginTransaction())
                using(var command = BuildCommand(
                    _schema.ReadAll,
                    transaction,
                    Parameters.Count(maxCount + 1),
                    Parameters.Position(fromPositionExclusive),
                    Parameters.ReadDirection(ReadDirection.Forward),
                    Parameters.Prefetch(prefetch)))
                using(var reader = await command.ExecuteReaderAsync(cancellationToken).NotOnCapturedContext())
                {
                    if(!reader.HasRows)
                    {
                        return new ReadAllPage(
                            fromPositionExclusive,
                            fromPositionExclusive,
                            true,
                            ReadDirection.Forward,
                            readNext,
                            Array.Empty<StreamMessage>());
                    }

                    var messages = new List<StreamMessage>();

                    while(await reader.ReadAsync(cancellationToken).NotOnCapturedContext())
                    {
                        if(messages.Count == maxCount)
                        {
                            messages.Add(default(StreamMessage));
                        }
                        else
                        {
                            var streamIdInfo = new StreamIdInfo(reader.GetString(0));
                            messages.Add(ReadStreamMessage(reader, streamIdInfo.PostgresqlStreamId, prefetch));
                        }
                    }

                    bool isEnd = true;

                    if(messages.Count == maxCount + 1) // An extra row was read, we're not at the end
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
            var ordinal = fromPositionExclusive == Position.End ? long.MaxValue : fromPositionExclusive;

            using(var connection = _createConnection())
            {
                await connection.OpenAsync(cancellationToken).NotOnCapturedContext();
                using(var transaction = connection.BeginTransaction())
                using(var command = BuildCommand(
                    _schema.ReadAll,
                    transaction,
                    Parameters.Count(maxCount + 1),
                    Parameters.Position(ordinal),
                    Parameters.ReadDirection(ReadDirection.Backward),
                    Parameters.Prefetch(prefetch)))
                using(var reader = await command.ExecuteReaderAsync(cancellationToken).NotOnCapturedContext())
                {
                    if(!reader.HasRows)
                    {
                        // When reading backwards and there are no more items, then next position is LongPosition.Start,
                        // regardles of what the fromPosition is.
                        return new ReadAllPage(
                            Position.Start,
                            Position.Start,
                            true,
                            ReadDirection.Backward,
                            readNext,
                            Array.Empty<StreamMessage>());
                    }

                    var messages = new List<StreamMessage>();

                    long lastOrdinal = 0;
                    while(await reader.ReadAsync(cancellationToken).NotOnCapturedContext())
                    {
                        var streamIdInfo = new StreamIdInfo(reader.GetString(0));
                        messages.Add(ReadStreamMessage(reader, streamIdInfo.PostgresqlStreamId, prefetch));

                        lastOrdinal = reader.GetInt64(3);
                    }

                    bool isEnd = true;
                    var nextPosition = lastOrdinal;

                    if(messages.Count == maxCount + 1) // An extra row was read, we're not at the end
                    {
                        isEnd = false;
                        messages.RemoveAt(maxCount);
                    }

                    fromPositionExclusive = messages.Count > 0 ? messages[0].Position : 0;

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