namespace SqlStreamStore.V1
{
    using System;
    using System.Collections.Generic;
    using System.Data;
    using System.Data.Common;
    using System.Linq;
    using System.Threading;
    using System.Threading.Tasks;
    using MySql.Data.MySqlClient;
    using SqlStreamStore.V1.Infrastructure;
    using SqlStreamStore.V1.MySqlScripts;
    using SqlStreamStore.V1.Streams;

    partial class MySqlStreamStore
    {
        protected override async Task<ReadStreamPage> ReadStreamForwardsInternal(
            string streamId,
            int start,
            int count,
            bool prefetch,
            ReadNextStreamPage readNext,
            CancellationToken cancellationToken)
        {
            using(var connection = await OpenConnection(cancellationToken))
            using(var transaction = await connection
                .BeginTransactionAsync(cancellationToken)
                .NotOnCapturedContext())
            {
                var streamIdInfo = new StreamIdInfo(streamId);

                return await ReadStreamInternal(
                    streamIdInfo.MySqlStreamId,
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
            using(var connection = await OpenConnection(cancellationToken))
            using(var transaction = await connection
                .BeginTransactionAsync(cancellationToken)
                .NotOnCapturedContext())
            {
                var streamIdInfo = new StreamIdInfo(streamId);

                return await ReadStreamInternal(
                    streamIdInfo.MySqlStreamId,
                    fromVersionInclusive,
                    count,
                    ReadDirection.Backward,
                    prefetch,
                    readNext,
                    transaction,
                    cancellationToken);
            }
        }

        private async Task<ReadStreamPage> ReadStreamInternal(
            MySqlStreamId streamId,
            int start,
            int count,
            ReadDirection direction,
            bool prefetch,
            ReadNextStreamPage readNext,
            MySqlTransaction transaction,
            CancellationToken cancellationToken)
        {
            // If the count is int.MaxValue, TSql will see it as a negative number. 
            // Users shouldn't be using int.MaxValue in the first place anyway.
            count = count == int.MaxValue ? count - 1 : count;

            // To read backwards from end, need to use int MaxValue
            var streamVersion = start == StreamVersion.End ? int.MaxValue : start;

            var messages = new List<(StreamMessage message, int? maxAge)>();

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

            using(var command = BuildStoredProcedureCall(
                _schema.Read,
                transaction,
                Parameters.StreamId(streamId),
                Parameters.Count(count + 1),
                Parameters.Version(streamVersion),
                Parameters.ReadDirection(direction),
                Parameters.Prefetch(prefetch)))
            using(var reader = await command
                .ExecuteReaderAsync(CommandBehavior.SequentialAccess, cancellationToken)
                .NotOnCapturedContext())
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
                var maxAge = reader.IsDBNull(2)
                    ? default
                    : reader.GetInt32(2);

                await reader.NextResultAsync(cancellationToken).NotOnCapturedContext();

                while(await reader.ReadAsync(cancellationToken).NotOnCapturedContext())
                {
                    messages.Add((await ReadStreamMessage(reader, streamId, prefetch), maxAge));
                }

                var isEnd = true;

                if(messages.Count == count + 1)
                {
                    isEnd = false;
                    messages.RemoveAt(count);
                }

                var filteredMessages = FilterExpired(messages);

                return new ReadStreamPage(
                    streamId.IdOriginal,
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

        private async Task<StreamMessage> ReadStreamMessage(
            DbDataReader reader,
            MySqlStreamId streamId,
            bool prefetch)
        {
            async Task<string> ReadString(int ordinal)
            {
                if(reader.IsDBNull(ordinal))
                {
                    return null;
                }

                using(var textReader = reader.GetTextReader(ordinal))
                {
                    return await textReader.ReadToEndAsync().NotOnCapturedContext();
                }
            }

            var messageId = reader.GetGuid(1);
            var streamVersion = reader.GetInt32(2);
            var position = reader.GetInt64(3);
            var createdUtc = reader.GetDateTime(4);
            var type = reader.GetString(5);
            var jsonMetadata = await ReadString(6);

            if(prefetch)
            {
                return new StreamMessage(
                    streamId.IdOriginal,
                    messageId,
                    streamVersion,
                    position,
                    createdUtc,
                    type,
                    jsonMetadata,
                    await ReadString(7));
            }

            return new StreamMessage(
                streamId.IdOriginal,
                messageId,
                streamVersion,
                position,
                createdUtc,
                type,
                jsonMetadata,
                ct => GetJsonData(streamId, streamVersion)(ct));
        }

        protected override async Task<long> ReadHeadPositionInternal(CancellationToken cancellationToken)
        {
            using(var connection = await OpenConnection(cancellationToken))
            using(var transaction = connection.BeginTransaction())
            using(var command = BuildStoredProcedureCall(_schema.ReadAllHeadPosition, transaction))
            {
                var result = await command.ExecuteScalarAsync(cancellationToken).NotOnCapturedContext();

                return result == DBNull.Value ? Position.End : ConvertPosition.FromMySqlToStreamStore((long) result);
            }
        }
    }
}