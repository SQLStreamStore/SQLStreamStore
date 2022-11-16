namespace SqlStreamStore
{
    using System;
    using System.Collections.Generic;
    using System.Collections.ObjectModel;
    using System.Data;
    using System.Data.Common;
    using System.Linq;
    using System.Text;
    using System.Threading;
    using System.Threading.Tasks;
    using Npgsql;
    using SqlStreamStore.PgSqlScripts;
    using SqlStreamStore.Streams;

    partial class PostgresStreamStore
    {
        private async Task<ReadStreamPage> ReadStreamInternal(
            PostgresqlStreamId streamId,
            int start,
            int count,
            ReadDirection direction,
            bool prefetch,
            ReadNextStreamPage readNext,
            NpgsqlTransaction transaction,
            CancellationToken cancellationToken)
        {
            // If the count is int.MaxValue, TSql will see it as a negative number.
            // Users shouldn't be using int.MaxValue in the first place anyway.
            count = count == int.MaxValue ? count - 1 : count;

            // To read backwards from end, need to use int MaxValue
            var streamVersion = start == StreamVersion.End ? int.MaxValue : start;

            var messages = new List<StreamMessage>();

            Func<ReadOnlyCollection<StreamMessage>, int, int> getNextVersion;

            if(direction == ReadDirection.Forward)
            {
                getNextVersion = (events, lastVersion) =>
                {
                    if(events.Any())
                    {
                        return events.Last().StreamVersion + 1;
                    }

                    return lastVersion + 1;
                };
            }
            else
            {
                getNextVersion = (events, lastVersion) =>
                {
                    if(events.Any())
                    {
                        return events.Last().StreamVersion - 1;
                    }

                    return -1;
                };
            }

            var refcursorSql = new StringBuilder();

            using(var command = BuildFunctionCommand(_schema.Read,
                      transaction,
                      Parameters.StreamId(streamId),
                      Parameters.Count(count + 1),
                      Parameters.Version(streamVersion),
                      Parameters.ReadDirection(direction),
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
                    return new ReadStreamPage(streamId.IdOriginal, PageReadStatus.StreamNotFound, start, -1, -1, -1, direction, true, readNext);
                }

                if(messages.Count == count)
                {
                    messages.Add(default);
                }

                await reader.ReadAsync(cancellationToken).ConfigureAwait(false);

                var lastVersion = reader.GetInt32(0);
                var lastPosition = reader.GetInt64(1);
                var maxAge = reader.GetFieldValue<int?>(2);

                await reader.NextResultAsync(cancellationToken).ConfigureAwait(false);

                while(await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
                {
                    messages.Add(await ReadStreamMessage(reader, streamId, prefetch).ConfigureAwait(false));
                }

                var isEnd = true;

                if(messages.Count == count + 1)
                {
                    isEnd = false;
                    messages.RemoveAt(count);
                }

                var readOnlyMessages = messages.AsReadOnly();

                var filteredMessages = maxAge.HasValue
                    ? FilterExpired(readOnlyMessages, new ReadOnlyDictionary<string, int>(new Dictionary<string, int> { { streamId.IdOriginal, maxAge.Value } }))
                    : readOnlyMessages;

                return new ReadStreamPage(streamId.IdOriginal,
                    PageReadStatus.Success,
                    start,
                    getNextVersion(filteredMessages, lastVersion),
                    lastVersion,
                    lastPosition,
                    direction,
                    isEnd,
                    readNext,
                    filteredMessages.ToArray());
            }
        }

        protected override async Task<ReadStreamPage> ReadStreamForwardsInternal(string streamId, int start, int count, bool prefetch, ReadNextStreamPage readNext, CancellationToken cancellationToken)
        {
            var streamIdInfo = new StreamIdInfo(streamId);

            using(var connection = await OpenConnection(cancellationToken).ConfigureAwait(false))
            using(var transaction = await connection.BeginTransactionAsync(cancellationToken).ConfigureAwait(false))
            {
                return await ReadStreamInternal(streamIdInfo.PostgresqlStreamId, start, count, ReadDirection.Forward, prefetch, readNext, transaction, cancellationToken).ConfigureAwait(false);
            }
        }

        protected override async Task<ReadStreamPage> ReadStreamBackwardsInternal(
            string streamId,
            int fromVersionInclusive,
            int count,
            bool prefetch,
            ReadNextStreamPage readNext,
            CancellationToken cancellationToken)
        {
            var streamIdInfo = new StreamIdInfo(streamId);

            using(var connection = await OpenConnection(cancellationToken).ConfigureAwait(false))
            using(var transaction = await connection.BeginTransactionAsync(cancellationToken).ConfigureAwait(false))
            {
                return await ReadStreamInternal(streamIdInfo.PostgresqlStreamId, fromVersionInclusive, count, ReadDirection.Backward, prefetch, readNext, transaction, cancellationToken)
                    .ConfigureAwait(false);
            }
        }

        private async Task<StreamMessage> ReadStreamMessage(DbDataReader reader, PostgresqlStreamId streamId, bool prefetch)
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
                return new StreamMessage(streamId.IdOriginal, messageId, streamVersion, position, createdUtc, type, jsonMetadata, await ReadString(7).ConfigureAwait(false));
            }

            return new StreamMessage(streamId.IdOriginal, messageId, streamVersion, position, createdUtc, type, jsonMetadata, ct => GetJsonData(streamId, streamVersion)(ct));
        }

        protected override async Task<long> ReadHeadPositionInternal(CancellationToken cancellationToken)
        {
            using(var connection = await OpenConnection(cancellationToken).ConfigureAwait(false))
            using(var transaction = await connection.BeginTransactionAsync(cancellationToken).ConfigureAwait(false))
            using(var command = BuildFunctionCommand(_schema.ReadAllHeadPosition, transaction))
            {
                var result = await command.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false);

                return result == DBNull.Value ? Position.End : (long) result;
            }
        }

        protected override async Task<long> ReadStreamHeadPositionInternal(string streamId, CancellationToken cancellationToken)
        {
            using(var connection = await OpenConnection(cancellationToken).ConfigureAwait(false))
            using(var transaction = await connection.BeginTransactionAsync(cancellationToken).ConfigureAwait(false))
            using(var command = BuildFunctionCommand(_schema.ReadStreamHeadPosition, transaction, Parameters.StreamId(new PostgresqlStreamId(streamId))))
            {
                var result = await command.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false);

                return result == DBNull.Value ? Position.End : (long) result;
            }
        }

        protected override async Task<int> ReadStreamHeadVersionInternal(string streamId, CancellationToken cancellationToken)
        {
            using(var connection = await OpenConnection(cancellationToken).ConfigureAwait(false))
            using(var transaction = await connection.BeginTransactionAsync(cancellationToken).ConfigureAwait(false))
            using(var command = BuildFunctionCommand(_schema.ReadStreamHeadVersion, transaction, Parameters.StreamId(new PostgresqlStreamId(streamId))))
            {
                var result = await command.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false);

                return result == DBNull.Value ? StreamVersion.End : (int) result;
            }
        }
    }
}