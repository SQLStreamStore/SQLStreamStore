namespace SqlStreamStore
{
    using System;
    using System.Collections.Generic;
    using System.Data;
    using System.Data.Common;
    using System.Linq;
    using System.Threading;
    using System.Threading.Tasks;
    using MySqlConnector;
    using SqlStreamStore.MySqlScripts;
    using SqlStreamStore.Streams;

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
            try
            {
                using(var connection = await OpenConnection(cancellationToken))
                using(var transaction = await connection
                    .BeginTransactionAsync(cancellationToken)
                    .ConfigureAwait(false))
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
            catch(MySqlException exception) when(exception.InnerException is ObjectDisposedException disposedException)
            {
                throw new ObjectDisposedException(disposedException.Message, exception);
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
            try
            {
                using(var connection = await OpenConnection(cancellationToken))
                using(var transaction = await connection
                    .BeginTransactionAsync(cancellationToken)
                    .ConfigureAwait(false))
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
            catch(MySqlException exception) when(exception.InnerException is ObjectDisposedException disposedException)
            {
                throw new ObjectDisposedException(disposedException.Message, exception);
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
            string procedure;
            Func<List<StreamMessage>, int, int> getNextVersion;

            if(direction == ReadDirection.Forward)
            {
                procedure = prefetch ? _schema.ReadStreamForwardsWithData : _schema.ReadStreamForwards;
                getNextVersion = (events, lastVersion) => events.Any()
                    ? events.Last().StreamVersion + 1
                    : lastVersion + 1;
            }
            else
            {
                procedure = prefetch ? _schema.ReadStreamBackwardsWithData : _schema.ReadStreamBackwards;
                getNextVersion = (events, lastVersion) => events.Any()
                    ? events.Last().StreamVersion - 1
                    : -1;
            }

            using(var command = BuildStoredProcedureCall(
                procedure,
                transaction,
                Parameters.StreamId(streamId),
                Parameters.Count(count + 1),
                Parameters.Version(streamVersion)))
            using(var reader = await command
                .ExecuteReaderAsync(CommandBehavior.SequentialAccess, cancellationToken)
                .ConfigureAwait(false))
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

                await reader.ReadAsync(cancellationToken).ConfigureAwait(false);

                var lastVersion = reader.GetInt32(0);
                var lastPosition = reader.GetInt64(1);
                var maxAge = reader.IsDBNull(2)
                    ? default
                    : reader.GetInt32(2);

                await reader.NextResultAsync(cancellationToken).ConfigureAwait(false);

                while(await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
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
                if(await reader.IsDBNullAsync(ordinal).ConfigureAwait(false))
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
            try
            {
                using(var connection = await OpenConnection(cancellationToken))
                using(var transaction = await connection.BeginTransactionAsync(cancellationToken).ConfigureAwait(false))
                using(var command = BuildStoredProcedureCall(_schema.ReadAllHeadPosition, transaction))
                {
                    var result = await command.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false);

                    return result == DBNull.Value
                        ? Position.End
                        : ConvertPosition.FromMySqlToStreamStore((long) result);
                }
            }
            catch(MySqlException exception) when(exception.InnerException is ObjectDisposedException disposedException)
            {
                throw new ObjectDisposedException(disposedException.Message, exception);
            }
        }

        protected override async Task<long> ReadStreamHeadPositionInternal(string streamId, CancellationToken cancellationToken)
        {
            try
            {
                using(var connection = await OpenConnection(cancellationToken))
                using(var transaction = connection.BeginTransaction())
                using(var command = BuildStoredProcedureCall(_schema.ReadStreamHeadPosition, transaction, Parameters.StreamId(new MySqlStreamId(streamId))))
                {
                    var result = await command.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false);

                    return result == null
                        ? Position.End
                        : ConvertPosition.FromMySqlToStreamStore((long) result);
                }
            }
            catch(MySqlException exception) when(exception.InnerException is ObjectDisposedException disposedException)
            {
                throw new ObjectDisposedException(disposedException.Message, exception);
            }
        }

        protected override async Task<int> ReadStreamHeadVersionInternal(string streamId, CancellationToken cancellationToken)
        {
            try
            {
                using(var connection = await OpenConnection(cancellationToken))
                using(var transaction = connection.BeginTransaction())
                using(var command = BuildStoredProcedureCall(_schema.ReadStreamHeadVersion, transaction, Parameters.StreamId(new MySqlStreamId(streamId))))
                {
                    var result = await command.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false);

                    return (int?) result ?? StreamVersion.End;
                }
            }
            catch(MySqlException exception) when(exception.InnerException is ObjectDisposedException disposedException)
            {
                throw new ObjectDisposedException(disposedException.Message, exception);
            }
        }
    }
}
