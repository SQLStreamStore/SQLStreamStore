namespace SqlStreamStore
{
    using System;
    using System.Collections.Generic;
    using System.Data;
    using System.Data.Common;
    using System.Linq;
    using System.Text;
    using System.Threading;
    using System.Threading.Tasks;

    using Npgsql;

    using SqlStreamStore.PgSqlScripts;
    using SqlStreamStore.Streams;

    public class PostgresReadAllPage : ReadAllPage<PostgresReadAllPage>
    {
        public List<long> TransactionIds { get; }
        public PostgresReadAllPage(
            long fromPosition,
            long nextPosition,
            bool isEnd,
            ReadDirection direction,
            ReadNextAllPage<PostgresReadAllPage> readNext,
            StreamMessage[] messages = null,
            string transactionSnapshot = null) : base(fromPosition, nextPosition, isEnd, direction, readNext, messages)
        {
            TransactionIds = ParseTransactionSnapshot(transactionSnapshot);
        }

        private static List<long> ParseTransactionSnapshot(string transactionSnapshot)
        {
            if (!string.IsNullOrWhiteSpace(transactionSnapshot))
            {
                var splitResult = transactionSnapshot.Split(':');

                if(splitResult.Length > 2 && !string.IsNullOrWhiteSpace(splitResult[2]))
                {
                    return splitResult[2].Split(',').Select(x => Convert.ToInt64(x)).ToList();
                }
            }
            
            return new List<long>();
        }
    }

    partial class PostgresStreamStore
    {
        protected override async Task<PostgresReadAllPage> ReadAllForwardsInternal(
            long fromPositionExclusive,
            int maxCount,
            bool prefetch,
            ReadNextAllPage<PostgresReadAllPage> readNext,
            CancellationToken cancellationToken,
            long fromMaxPositionInclusive = -1)
        {
            maxCount = maxCount == int.MaxValue ? maxCount - 1 : maxCount;

            var refcursorSql = new StringBuilder();

            using (var connection = await OpenConnection(cancellationToken))
            using (var transaction = connection.BeginTransaction())
            {
                using (var command = BuildFunctionCommand(
                _schema.ReadAll,
                transaction,
                Parameters.Count(maxCount + 1),
                Parameters.Position(fromPositionExclusive),
                Parameters.ReadDirection(ReadDirection.Forward),
                Parameters.Prefetch(prefetch),
                Parameters.Position(fromMaxPositionInclusive)))
                using (var reader = await command
                    .ExecuteReaderAsync(CommandBehavior.SequentialAccess, cancellationToken)
                    .ConfigureAwait(false))
                {
                    while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
                    {
                        refcursorSql.AppendLine(Schema.FetchAll(reader.GetString(0)));
                    }
                }

                using (var command = new NpgsqlCommand(refcursorSql.ToString(), transaction.Connection, transaction))
                using (var reader = await command
                    .ExecuteReaderAsync(CommandBehavior.SequentialAccess, cancellationToken)
                    .ConfigureAwait(false))
                {

                    if (!reader.HasRows)
                    {
                        return new PostgresReadAllPage(
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
                                await ReadAllStreamMessage(reader, streamIdInfo.PostgresqlStreamId, prefetch);
                            messages.Add((message, maxAge));
                        }
                    }

                    string transactionSnapshot = string.Empty;
                    await reader.NextResultAsync(cancellationToken).ConfigureAwait(false);
                    while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
                    {
                        transactionSnapshot = await reader.GetFieldValueAsync<string>(0, cancellationToken);
                    }

                    bool isEnd = true;

                    if (messages.Count == maxCount + 1) // An extra row was read, we're not at the end
                    {
                        isEnd = false;
                        messages.RemoveAt(maxCount);
                    }

                    var filteredMessages = FilterExpired(messages);

                    var nextPosition = filteredMessages[filteredMessages.Count - 1].Position + 1;

                    return new PostgresReadAllPage(
                        fromPositionExclusive,
                        nextPosition,
                        isEnd,
                        ReadDirection.Forward,
                        readNext,
                        filteredMessages.ToArray(),
                        transactionSnapshot);
                }
            }
        }

        protected override async Task<PostgresReadAllPage> ReadAllBackwardsInternal(
            long fromPositionExclusive,
            int maxCount,
            bool prefetch,
            ReadNextAllPage<PostgresReadAllPage> readNext,
            CancellationToken cancellationToken)
        {
            maxCount = maxCount == int.MaxValue ? maxCount - 1 : maxCount;
            var ordinal = fromPositionExclusive == Position.End ? long.MaxValue : fromPositionExclusive;

            var refcursorSql = new StringBuilder();

            using (var connection = await OpenConnection(cancellationToken))
            using (var transaction = connection.BeginTransaction())
            {
                using (var command = BuildFunctionCommand(
                    _schema.ReadAll,
                    transaction,
                    Parameters.Count(maxCount + 1),
                    Parameters.Position(ordinal),
                    Parameters.ReadDirection(ReadDirection.Backward),
                    Parameters.Prefetch(prefetch),
                    Parameters.Position(-1)))
                {
                    using (var reader = await command
                        .ExecuteReaderAsync(CommandBehavior.SequentialAccess, cancellationToken)
                        .ConfigureAwait(false))

                    {
                        while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
                        {
                            refcursorSql.AppendLine(Schema.FetchAll(reader.GetString(0)));
                        }
                    }
                }

                using (var command = new NpgsqlCommand(refcursorSql.ToString(), transaction.Connection, transaction))
                using (var reader = await command
                    .ExecuteReaderAsync(CommandBehavior.SequentialAccess, cancellationToken)
                    .ConfigureAwait(false))
                {
                    if (!reader.HasRows)
                    {
                        // When reading backwards and there are no more items, then next position is LongPosition.Start,
                        // regardless of what the fromPosition is.
                        return new PostgresReadAllPage(
                            Position.Start,
                            Position.Start,
                            true,
                            ReadDirection.Backward,
                            readNext,
                            Array.Empty<StreamMessage>());
                    }

                    var messages = new List<(StreamMessage message, int? maxAge)>();

                    long lastOrdinal = 0;
                    while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
                    {
                        var streamIdInfo = new StreamIdInfo(reader.GetString(0));
                        var (message, maxAge, position) = await ReadAllStreamMessage(reader, streamIdInfo.PostgresqlStreamId, prefetch);
                        messages.Add((message, maxAge));

                        lastOrdinal = position;
                    }

                    bool isEnd = true;
                    var nextPosition = lastOrdinal;

                    if (messages.Count == maxCount + 1) // An extra row was read, we're not at the end
                    {
                        isEnd = false;
                        messages.RemoveAt(maxCount);
                    }

                    var filteredMessages = FilterExpired(messages);

                    fromPositionExclusive = filteredMessages.Count > 0 ? filteredMessages[0].Position : 0;

                    return new PostgresReadAllPage(
                        fromPositionExclusive,
                        nextPosition,
                        isEnd,
                        ReadDirection.Backward,
                        readNext,
                        filteredMessages.ToArray());
                }
            }
        }

        private async Task<(StreamMessage message, int? maxAge, long position)> ReadAllStreamMessage(
            DbDataReader reader,
            PostgresqlStreamId streamId,
            bool prefetch)
        {
            async Task<string> ReadString(int ordinal)
            {
                if (reader.IsDBNull(ordinal))
                {
                    return null;
                }

                using (var textReader = reader.GetTextReader(ordinal))
                {
                    return await textReader.ReadToEndAsync().ConfigureAwait(false);
                }
            }

            var messageId = reader.GetGuid(1);
            var streamVersion = reader.GetInt32(2);
            var position = reader.GetInt64(3);
            var createdUtc = reader.GetDateTime(4);
            var type = reader.GetString(5);
            var jsonMetadata = await ReadString(6);

            if (prefetch)
            {
                return (
                    new StreamMessage(
                        streamId.IdOriginal,
                        messageId,
                        streamVersion,
                        position,
                        createdUtc,
                        type,
                        jsonMetadata,
                        await ReadString(7)),
                    reader.GetFieldValue<int?>(8),
                    position);
            }

            return (
                new StreamMessage(
                    streamId.IdOriginal,
                    messageId,
                    streamVersion,
                    position,
                    createdUtc,
                    type,
                    jsonMetadata,
                    ct => GetJsonData(streamId, streamVersion)(ct)),
                reader.GetFieldValue<int?>(8),
                position);
        }
    }
}
