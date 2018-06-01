namespace SqlStreamStore
{
    using System;
    using System.Collections.Generic;
    using System.Data;
    using System.Linq;
    using System.Text;
    using System.Threading;
    using System.Threading.Tasks;
    using Npgsql;
    using SqlStreamStore.Infrastructure;
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

            Func<List<StreamMessage>, int, int> getNextVersion;

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

            using(var command = BuildFunctionCommand(
                _schema.Read,
                transaction,
                Parameters.StreamId(streamId),
                Parameters.Count(count + 1),
                Parameters.Version(streamVersion),
                Parameters.ReadDirection(direction),
                Parameters.Prefetch(prefetch)))
            using(var reader = await command.ExecuteReaderAsync(CommandBehavior.SequentialAccess, cancellationToken)
                .NotOnCapturedContext())
            {
                while(await reader.ReadAsync(cancellationToken).NotOnCapturedContext())
                {
                    refcursorSql.AppendLine(Schema.FetchAll(reader.GetString(0)));
                }
            }

            using(var command = new NpgsqlCommand(refcursorSql.ToString(), transaction.Connection, transaction))
            using(var reader = await command.ExecuteReaderAsync(cancellationToken).NotOnCapturedContext())
            {
                if(!reader.HasRows)
                {
                    return new ReadStreamPage(
                        streamId.IdOriginal,
                        PageReadStatus.StreamNotFound,
                        start,
                        -1,
                        -1,
                        -1,
                        direction,
                        true,
                        readNext);
                }

                if(messages.Count == count)
                {
                    messages.Add(default);
                }

                await reader.ReadAsync(cancellationToken).NotOnCapturedContext();

                var lastVersion = reader.GetInt32(0);
                var lastPosition = reader.GetInt64(1);

                await reader.NextResultAsync(cancellationToken).NotOnCapturedContext();

                while(await reader.ReadAsync(cancellationToken).NotOnCapturedContext())
                {
                    messages.Add(ReadStreamMessage(reader, streamId, prefetch));
                }

                var isEnd = true;

                if(messages.Count == count + 1)
                {
                    isEnd = false;
                    messages.RemoveAt(count);
                }

                return new ReadStreamPage(
                    streamId.IdOriginal,
                    PageReadStatus.Success,
                    start,
                    getNextVersion(messages, lastVersion),
                    lastVersion,
                    lastPosition,
                    direction,
                    isEnd,
                    readNext,
                    messages.ToArray());
            }
        }

        protected override async Task<ReadStreamPage> ReadStreamForwardsInternal(
            string streamId,
            int start,
            int count,
            bool prefetch,
            ReadNextStreamPage readNext,
            CancellationToken cancellationToken)
        {
            var streamIdInfo = new StreamIdInfo(streamId);

            using(var connection = _createConnection())
            using(var transaction = await BeginTransaction(connection, cancellationToken))
            {
                return await ReadStreamInternal(streamIdInfo.PostgresqlStreamId,
                    start,
                    count,
                    ReadDirection.Forward,
                    prefetch,
                    readNext,
                    transaction,
                    cancellationToken);
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

            using(var connection = _createConnection())
            using(var transaction = await BeginTransaction(connection, cancellationToken))
            {
                return await ReadStreamInternal(streamIdInfo.PostgresqlStreamId,
                    fromVersionInclusive,
                    count,
                    ReadDirection.Backward,
                    prefetch,
                    readNext,
                    transaction,
                    cancellationToken);
            }
        }

        private StreamMessage ReadStreamMessage(IDataRecord reader, PostgresqlStreamId streamId, bool prefetch)
        {
            var streamVersion = reader.GetInt32(2);

            if(prefetch)
            {
                return new StreamMessage(
                    reader.GetString(0),
                    reader.GetGuid(1),
                    streamVersion,
                    reader.GetInt64(3),
                    reader.GetDateTime(4),
                    reader.GetString(5),
                    reader.GetString(6),
                    reader.GetString(7));
            }

            return new StreamMessage(
                reader.GetString(0),
                reader.GetGuid(1),
                streamVersion,
                reader.GetInt64(3),
                reader.GetDateTime(4),
                reader.GetString(5),
                reader.GetString(6),
                ct => GetJsonData(streamId, streamVersion)(ct));
        }

        protected override async Task<long> ReadHeadPositionInternal(CancellationToken cancellationToken)
        {
            using(var connection = _createConnection())
            using(var transaction = await BeginTransaction(connection, cancellationToken))
            using(var command = BuildFunctionCommand(_schema.ReadAllHeadPosition, transaction))
            {
                var result = await command.ExecuteScalarAsync(cancellationToken).NotOnCapturedContext();

                return result == DBNull.Value ? Position.End : (long) result;
            }
        }
    }
}