namespace SqlStreamStore
{
    using System;
    using System.Collections.Generic;
    using System.Collections.ObjectModel;
    using System.Data;
    using System.Data.Common;
    using System.Diagnostics;
    using System.Linq;
    using System.Text;
    using System.Threading;
    using System.Threading.Tasks;
    using Npgsql;
    using SqlStreamStore.Logging;
    using SqlStreamStore.PgSqlScripts;
    using SqlStreamStore.Streams;

    partial class PostgresStreamStore
    {
        protected override async Task<ReadAllPage> ReadAllForwardsInternal(long fromPositionExclusive, int maxCount, bool prefetch, ReadNextAllPage readNext, CancellationToken cancellationToken)
        {
            var correlation = Guid.NewGuid();

            maxCount = maxCount == int.MaxValue ? maxCount - 1 : maxCount;

            var (messages, maxAgeDict, transactionIdsInProgress, isEnd) = await ReadAllForwards(fromPositionExclusive, maxCount, prefetch, correlation, cancellationToken).ConfigureAwait(false);

            if(_settings.GapHandlingSettings != null)
            {
                var r = await HandleGaps(messages, maxAgeDict, transactionIdsInProgress, isEnd, fromPositionExclusive, maxCount, prefetch, correlation, cancellationToken).ConfigureAwait(false);

                isEnd = r.isEnd;
                messages = r.messages;
            }

            if(!messages.Any())
            {
                return new ReadAllPage(fromPositionExclusive, fromPositionExclusive, isEnd, ReadDirection.Forward, readNext, Array.Empty<StreamMessage>());
            }

            var filteredMessages = FilterExpired(messages, maxAgeDict);
            var nextPosition = filteredMessages[filteredMessages.Count - 1].Position + 1;

            return new ReadAllPage(fromPositionExclusive, nextPosition, isEnd, ReadDirection.Forward, readNext, filteredMessages.ToArray());
        }

        private async Task<(ReadOnlyCollection<StreamMessage> messages, ReadOnlyDictionary<string, int> maxAgeDict, bool isEnd)> HandleGaps(
            ReadOnlyCollection<StreamMessage> messages,
            ReadOnlyDictionary<string, int> maxAgeDict,
            TxIdList transactionsInProgress,
            bool isEnd,
            long fromPositionInclusive,
            int maxCount,
            bool prefetch,
            Guid correlation,
            CancellationToken cancellationToken)
        {
            var hasMessages = messages.Count > 0;
            var hasTransactionsInProgress = transactionsInProgress.Count > 0;

            // We do this as otherwise the Select always enumerates even when trace log is disabled.
            // When we retrieve high amount of messages this will impact the performance.
            if(Logger.IsTraceEnabled())
            {
                Logger.TraceFormat("Correlation: {correlation} | {messages} | Transactions in progress: {transactions}",
                    correlation,
                    hasMessages ? $"Count: {messages.Count} | {string.Join("|", messages.Select((x, i) => $"Position: {x.Position} Array index: {i}"))}" : "No messages",
                    transactionsInProgress);
            }
            
            if(!hasMessages && !hasTransactionsInProgress)
            {
                Logger.TraceFormat("Correlation: {correlation} | No messages found, no transactions in progress. We will return empty list of messages with isEnd to true", correlation, messages);
                return (new ReadOnlyCollection<StreamMessage>(new List<StreamMessage>()), new ReadOnlyDictionary<string, int>(new Dictionary<string, int>()), true);
            }

            if(!hasTransactionsInProgress)
            {
                Logger.TraceFormat("Correlation: {correlation} | No transactions in progress, no need for gap checking", correlation);
                return (messages, maxAgeDict, isEnd);
            }

            // It's possible the gaps still need to be formed
            // Wait until transactions are done
            // And restart the read (is done by returning an empty list with isEnd = false)
            if(!hasMessages)
            {
                Logger.TraceFormat("Correlation: {correlation} | Transactions in progress: {transactions} | But no messages found", correlation);

                await PollTransactions(correlation, transactionsInProgress, cancellationToken).ConfigureAwait(false);
                return (new ReadOnlyCollection<StreamMessage>(new List<StreamMessage>()), new ReadOnlyDictionary<string, int>(new Dictionary<string, int>()), false);
            }

            Logger.TraceFormat("Correlation: {correlation} | Danger zone! We have messages & transactions in progress, we need to start gap checking", correlation);

            // Check for gap between last page and this. 
            if(messages[0].Position != fromPositionInclusive)
            {
                Logger.TraceFormat(
                    "Correlation: {correlation} | fromPositionInclusive {fromPositionInclusive} does not match first position of received messages {position} | Transactions in progress: {transactions}",
                    correlation,
                    fromPositionInclusive,
                    transactionsInProgress);

                await PollTransactions(correlation, transactionsInProgress, cancellationToken).ConfigureAwait(false);

                return await ReadTrustedMessages(fromPositionInclusive, messages[messages.Count - 1].Position, maxCount, prefetch, correlation, cancellationToken).ConfigureAwait(false);
            }


            for(int i = 0; i < messages.Count - 1; i++)
            {
                var expectedNextPosition = messages[i].Position + 1;
                var actualPosition = messages[i + 1].Position;
                Logger.TraceFormat("Correlation: {correlation} | Gap checking. Expected position: {expectedNextPosition} | Actual position: {actualPosition}",
                    correlation,
                    expectedNextPosition,
                    actualPosition);

                if(expectedNextPosition != actualPosition)
                {
                    Logger.TraceFormat("Correlation: {correlation} | Gap detected", correlation);

                    await PollTransactions(correlation, transactionsInProgress, cancellationToken).ConfigureAwait(false);

                    return await ReadTrustedMessages(fromPositionInclusive, messages[messages.Count - 1].Position, maxCount, prefetch, correlation, cancellationToken).ConfigureAwait(false);
                }
            }

            return (messages, maxAgeDict, isEnd);
        }

        private async Task PollTransactions(Guid correlation, TxIdList transactionsInProgress, CancellationToken cancellationToken)
        {
            Logger.TraceFormat("Correlation: {correlation} | Transactions in progress: {transactions} | Gaps might be filled, start comparing", correlation, transactionsInProgress);

            bool stillInProgress;
            var count = 0;
            var delayTime = 0;
            var totalTime = 0L;

            var sw = Stopwatch.StartNew();
            do
            {
                if(totalTime > _settings.GapHandlingSettings.MinimumWarnTime)
                {
                    Logger.ErrorFormat(
                        "Correlation: {correlation} | Transactions in progress: {transactions} | Possible DEADLOCK! One of the transactions is in progress for longer than {totalTime}ms",
                        correlation,
                        transactionsInProgress,
                        _settings.GapHandlingSettings.MinimumWarnTime);
                }

                if(totalTime > _settings.GapHandlingSettings.SkipTime)
                {
                    Logger.ErrorFormat(
                        "Correlation: {correlation} | Transactions in progress: {transactions} | Possible SKIPPED EVENT as we will stop waiting for in progress transactions! One of the transactions is in progress for longer than {totalTime}ms",
                        correlation,
                        transactionsInProgress,
                        _settings.GapHandlingSettings.SkipTime);
                    return;
                }

                if(delayTime > 0)
                {
                    Logger.TraceFormat("Correlation: {correlation} | Delay 'PollTransactions' for {delayTime}ms", correlation, delayTime);
                    await Task.Delay(delayTime, cancellationToken).ConfigureAwait(false);
                }

                if(count % 5 == 0)
                    delayTime += 10;

                stillInProgress = await ReadAnyTransactionsInProgress(transactionsInProgress, cancellationToken).ConfigureAwait(false);
                Logger.TraceFormat("Correlation: {correlation} | Transactions still pending. Query 'ReadAnyTransactionsInProgress' took: {timeTaken}ms", correlation, sw.ElapsedMilliseconds);

                totalTime += sw.ElapsedMilliseconds;
                count++;
                sw.Restart();

                Logger.TraceFormat("Correlation: {correlation} | State 'PollTransactions' | count: {count} | delayTime: {delayTime} | totalTime: {totalTime}",
                    correlation,
                    count,
                    delayTime,
                    totalTime);
            } while(stillInProgress);
        }

        private async Task<(ReadOnlyCollection<StreamMessage>, ReadOnlyDictionary<string, int>, bool)> ReadTrustedMessages(
            long fromPositionInclusive,
            long toPositionInclusive,
            int maxCount,
            bool prefetch,
            Guid correlation,
            CancellationToken cancellationToken)
        {
            Logger.TraceFormat("Correlation: {correlation} | Read trusted message initiated", correlation);

            var (messages, maxAgeDict, _, isEnd) = await ReadAllForwards(fromPositionInclusive, maxCount, prefetch, correlation, cancellationToken).ConfigureAwait(false);
            
            Logger.TraceFormat("Correlation: {correlation} | Filter messages from {fromPositionInclusive} to {toPositionInclusive}", correlation, fromPositionInclusive, toPositionInclusive);

            var messageToReturn = messages.Where(x => x.Position >= fromPositionInclusive && x.Position <= toPositionInclusive).ToList();

            if(isEnd && messageToReturn.Count <= messages.Count)
                isEnd = false;

            Logger.TraceFormat("Correlation: {correlation} | IsEnd: {isEnd} | FilteredCount: {filteredCount} | TotalCount: {totalCount}", correlation, isEnd, messageToReturn.Count, messages.Count);

            return (messageToReturn.AsReadOnly(), new ReadOnlyDictionary<string, int>(maxAgeDict), isEnd);
        }

        private async Task<bool> ReadAnyTransactionsInProgress(TxIdList transactionIds, CancellationToken cancellationToken)
        {
            using(var connection = await OpenConnection(cancellationToken).ConfigureAwait(false))
            using(var transaction = await connection.BeginTransactionAsync(cancellationToken).ConfigureAwait(false))
            using(var command = BuildFunctionCommand(_schema.ReadAnyTransactionsInProgress, transaction, Parameters.Name(connection.Database), Parameters.TransactionIds(transactionIds)))
            {
                var result = await command.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false) as bool?;

                return result ?? false;
            }
        }

        private async Task<(ReadOnlyCollection<StreamMessage> messages, ReadOnlyDictionary<string, int> maxAgeDict, TxIdList transactionIdsInProgress, bool isEnd)> ReadAllForwards(
            long fromPositionExclusive,
            int maxCount,
            bool prefetch,
            Guid correlation,
            CancellationToken cancellationToken)
        {
            var sw = Stopwatch.StartNew();

            var refcursorSql = new StringBuilder();

            using(var connection = await OpenConnection(cancellationToken).ConfigureAwait(false))
            using(var transaction = await connection.BeginTransactionAsync(cancellationToken).ConfigureAwait(false))
            {
                using(var command = BuildFunctionCommand(_schema.ReadAll,
                          transaction,
                          Parameters.Count(maxCount + 1),
                          Parameters.Position(fromPositionExclusive),
                          Parameters.ReadDirection(ReadDirection.Forward),
                          Parameters.Prefetch(prefetch)))
                using(var reader = await command.ExecuteReaderAsync(CommandBehavior.SequentialAccess, cancellationToken).ConfigureAwait(false))
                {
                    while(await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
                    {
                        refcursorSql.AppendLine(Schema.FetchAll(reader.GetString(0)));
                    }
                }

                using(var command = new NpgsqlCommand(refcursorSql.ToString(), transaction.Connection, transaction))
                using(var reader = await command.ExecuteReaderAsync(CommandBehavior.SequentialAccess, cancellationToken).ConfigureAwait(false))
                {
                    if(!reader.HasRows)
                    {
                        return (new List<StreamMessage>().AsReadOnly(), new ReadOnlyDictionary<string, int>(new Dictionary<string, int>()), new TxIdList(), true);
                    }

                    var messages = new List<StreamMessage>();
                    var maxAgeDict = new Dictionary<string, int>();
                    var isEnd = true;

                    while(await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
                    {
                        if(messages.Count == maxCount)
                            isEnd = false;
                        else
                        {
                            var streamIdInfo = new StreamIdInfo(reader.GetString(0));
                            var (message, maxAge, _) = await ReadAllStreamMessage(reader, streamIdInfo.PostgresqlStreamId, prefetch).ConfigureAwait(false);

                            if(maxAge.HasValue)
                            {
                                if(!maxAgeDict.ContainsKey(message.StreamId))
                                {
                                    maxAgeDict.Add(message.StreamId, maxAge.Value);
                                }
                            }

                            messages.Add(message);
                        }
                    }

                    var transactionIdsInProgress = new TxIdList();
                    await reader.NextResultAsync(cancellationToken).ConfigureAwait(false);
                    while(await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
                    {
                        transactionIdsInProgress.Add(await reader.GetFieldValueAsync<long>(0, cancellationToken).ConfigureAwait(false));
                    }

                    Logger.TraceFormat("Correlation: {correlation} | Query 'ReadAllForwards' took: {timeTaken}ms", correlation, sw.ElapsedMilliseconds);

                    return (messages.AsReadOnly(), new ReadOnlyDictionary<string, int>(maxAgeDict), transactionIdsInProgress, isEnd);
                }
            }
        }

        protected override async Task<ReadAllPage> ReadAllBackwardsInternal(long fromPositionExclusive, int maxCount, bool prefetch, ReadNextAllPage readNext, CancellationToken cancellationToken)
        {
            maxCount = maxCount == int.MaxValue ? maxCount - 1 : maxCount;
            var ordinal = fromPositionExclusive == Position.End ? long.MaxValue : fromPositionExclusive;

            var refcursorSql = new StringBuilder();

            using(var connection = await OpenConnection(cancellationToken).ConfigureAwait(false))
            using(var transaction = await connection.BeginTransactionAsync(cancellationToken).ConfigureAwait(false))
            {
                using(var command = BuildFunctionCommand(_schema.ReadAll,
                          transaction,
                          Parameters.Count(maxCount + 1),
                          Parameters.Position(ordinal),
                          Parameters.ReadDirection(ReadDirection.Backward),
                          Parameters.Prefetch(prefetch)))
                using(var reader = await command.ExecuteReaderAsync(CommandBehavior.SequentialAccess, cancellationToken).ConfigureAwait(false))
                {
                    while(await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
                    {
                        refcursorSql.AppendLine(Schema.FetchAll(reader.GetString(0)));
                    }
                }


                using(var command = new NpgsqlCommand(refcursorSql.ToString(), transaction.Connection, transaction))
                using(var reader = await command.ExecuteReaderAsync(CommandBehavior.SequentialAccess, cancellationToken).ConfigureAwait(false))
                {
                    if(!reader.HasRows)
                    {
                        // When reading backwards and there are no more items, then next position is LongPosition.Start,
                        // regardless of what the fromPosition is.
                        return new ReadAllPage(Position.Start, Position.Start, true, ReadDirection.Backward, readNext, Array.Empty<StreamMessage>());
                    }

                    var messages = new List<StreamMessage>();
                    var maxAgeDict = new Dictionary<string, int>();

                    long lastOrdinal = 0;
                    while(await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
                    {
                        var streamIdInfo = new StreamIdInfo(reader.GetString(0));
                        var (message, maxAge, position) = await ReadAllStreamMessage(reader, streamIdInfo.PostgresqlStreamId, prefetch).ConfigureAwait(false);

                        if(maxAge.HasValue)
                        {
                            if(!maxAgeDict.ContainsKey(message.StreamId))
                            {
                                maxAgeDict.Add(message.StreamId, maxAge.Value);
                            }
                        }

                        messages.Add(message);

                        lastOrdinal = position;
                    }

                    bool isEnd = true;
                    var nextPosition = lastOrdinal;

                    if(messages.Count == maxCount + 1) // An extra row was read, we're not at the end
                    {
                        isEnd = false;
                        messages.RemoveAt(maxCount);
                    }

                    var filteredMessages = FilterExpired(messages.AsReadOnly(), new ReadOnlyDictionary<string, int>(maxAgeDict));

                    fromPositionExclusive = filteredMessages.Count > 0 ? filteredMessages[0].Position : 0;

                    return new ReadAllPage(fromPositionExclusive, nextPosition, isEnd, ReadDirection.Backward, readNext, filteredMessages.ToArray());
                }
            }
        }

        private async Task<(StreamMessage message, int? maxAge, long position)> ReadAllStreamMessage(DbDataReader reader, PostgresqlStreamId streamId, bool prefetch)
        {
            async Task<string> ReadString(int ordinal)
            {
                if(reader.IsDBNull(ordinal))
                {
                    return null;
                }

                using(var textReader = reader.GetTextReader(ordinal))
                {
                    return await textReader.ReadToEndAsync().ConfigureAwait(false);
                }
            }

            var messageId = reader.GetGuid(1);
            var streamVersion = reader.GetInt32(2);
            var position = reader.GetInt64(3);
            var createdUtc = reader.GetDateTime(4);
            var type = reader.GetString(5);
            var jsonMetadata = await ReadString(6).ConfigureAwait(false);

            if(prefetch)
            {
                return (new StreamMessage(streamId.IdOriginal, messageId, streamVersion, position, createdUtc, type, jsonMetadata, await ReadString(7).ConfigureAwait(false)),
                    reader.GetFieldValue<int?>(8), position);
            }

            return (new StreamMessage(streamId.IdOriginal, messageId, streamVersion, position, createdUtc, type, jsonMetadata, ct => GetJsonData(streamId, streamVersion)(ct)),
                reader.GetFieldValue<int?>(8), position);
        }
    }
}