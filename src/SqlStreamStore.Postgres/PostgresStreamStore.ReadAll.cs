namespace SqlStreamStore
{
    using System;
    using System.Collections.Generic;
    using System.Data;
    using System.Threading;
    using System.Threading.Tasks;
    using Npgsql;
    using NpgsqlTypes;
    using SqlStreamStore.Infrastructure;
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
                using(var command = new NpgsqlCommand(_schema.ReadAll, connection)
                {
                    CommandType = CommandType.StoredProcedure,
                    Parameters =
                    {
                        new NpgsqlParameter
                        {
                            NpgsqlDbType = NpgsqlDbType.Integer,
                            NpgsqlValue = maxCount + 1
                        },
                        new NpgsqlParameter
                        {
                            NpgsqlDbType = NpgsqlDbType.Bigint,
                            NpgsqlValue = fromPositionExclusive
                        },
                        new NpgsqlParameter
                        {
                            NpgsqlDbType = NpgsqlDbType.Boolean,
                            NpgsqlValue = true
                        },
                        new NpgsqlParameter
                        {
                            NpgsqlDbType = NpgsqlDbType.Boolean,
                            NpgsqlValue = prefetch
                        }
                    }
                })
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
                            messages.Add(
                                ReadStreamMessage(
                                    new StreamIdInfo(reader.GetString(0)).PostgresqlStreamId, reader, prefetch));
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
                using(var command = new NpgsqlCommand(_schema.ReadAll, connection)
                {
                    CommandType = CommandType.StoredProcedure,
                    Parameters =
                    {
                        new NpgsqlParameter
                        {
                            NpgsqlDbType = NpgsqlDbType.Integer,
                            NpgsqlValue = maxCount + 1
                        },
                        new NpgsqlParameter
                        {
                            NpgsqlDbType = NpgsqlDbType.Bigint,
                            NpgsqlValue = ordinal
                        },
                        new NpgsqlParameter
                        {
                            NpgsqlDbType = NpgsqlDbType.Boolean,
                            NpgsqlValue = false
                        },
                        new NpgsqlParameter
                        {
                            NpgsqlDbType = NpgsqlDbType.Boolean,
                            NpgsqlValue = prefetch
                        }
                    }
                })
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
                        messages.Add(
                            ReadStreamMessage(
                                new StreamIdInfo(reader.GetString(0)).PostgresqlStreamId, reader, prefetch));

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