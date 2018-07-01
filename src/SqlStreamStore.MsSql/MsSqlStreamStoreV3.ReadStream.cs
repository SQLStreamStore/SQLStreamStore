﻿namespace SqlStreamStore
{
    using System;
    using System.Collections.Generic;
    using System.Data.SqlClient;
    using System.Linq;
    using System.Threading;
    using System.Threading.Tasks;
    using SqlStreamStore.Streams;
    using SqlStreamStore.Infrastructure;

    public partial class MsSqlStreamStoreV3
    {
        protected override async Task<ReadStreamPage> ReadStreamForwardsInternal(
            string streamId,
            int start,
            int count,
            bool prefetch,
            ReadNextStreamPage readNext,
            CancellationToken cancellationToken)
        {
            using (var connection = _createConnection())
            {
                await connection.OpenAsync(cancellationToken).NotOnCapturedContext();
                var streamIdInfo = new StreamIdInfo(streamId);
                var(page, _) = await ReadStreamInternal(
                    streamIdInfo.SqlStreamId,
                    start,
                    count,
                    ReadDirection.Forward,
                    prefetch,
                    readNext,
                    connection,
                    null,
                    cancellationToken);
                return page;
            }
        }

        protected override async Task<ReadStreamPage> ReadStreamBackwardsInternal(
            string streamId,
            int start,
            int count,
            bool prefetch, 
            ReadNextStreamPage readNext,
            CancellationToken cancellationToken)
        {
            using (var connection = _createConnection())
            {
                await connection.OpenAsync(cancellationToken).NotOnCapturedContext();
                var streamIdInfo = new StreamIdInfo(streamId);
                var (page, _) = await ReadStreamInternal(
                    streamIdInfo.SqlStreamId,
                    start,
                    count,
                    ReadDirection.Backward,
                    prefetch,
                    readNext,
                    connection,
                    null,
                    cancellationToken);
                return page;
            }
        }

        private async Task<(ReadStreamPage, StreamMeta)> ReadStreamInternal(
            SqlStreamId sqlStreamId,
            int start,
            int count,
            ReadDirection direction,
            bool prefetch,
            ReadNextStreamPage readNext,
            SqlConnection connection,
            SqlTransaction transaction,
            CancellationToken cancellationToken)
        {
            // If the count is int.MaxValue, TSql will see it as a negative number. 
            // Users shouldn't be using int.MaxValue in the first place anyway.
            count = count == int.MaxValue ? count - 1 : count;

            // To read backwards from end, need to use int MaxValue
            var streamVersion = start == StreamVersion.End ? int.MaxValue : start;
            string commandText;
            Func<List<(StreamMessage, int?)>, int, int> getNextVersion;
            if(direction == ReadDirection.Forward)
            {
                commandText = prefetch ? _scripts.ReadStreamForwardWithData : _scripts.ReadStreamForward;
                getNextVersion = (events, lastVersion) => 
                    events.Any() 
                        ? events.Last().Item1.StreamVersion + 1 
                        : lastVersion + 1;
            }
            else
            {
                commandText = prefetch ? _scripts.ReadStreamBackwardWithData : _scripts.ReadStreamBackward;
                getNextVersion = (events, lastVersion) =>
                    events.Any() 
                        ? events.Last().Item1.StreamVersion - 1 
                        : -1;
            }

            using(var command = new SqlCommand(commandText, connection, transaction))
            {
                command.Parameters.AddWithValue("streamId", sqlStreamId.Id);
                command.Parameters.AddWithValue("count", count + 1); //Read extra row to see if at end or not
                command.Parameters.AddWithValue("streamVersion", streamVersion);

                using(var reader = await command.ExecuteReaderAsync(cancellationToken).NotOnCapturedContext())
                {
                    await reader.ReadAsync(cancellationToken).NotOnCapturedContext();
                    if(reader.IsDBNull(0))
                    {
                        return (new ReadStreamPage(
                              sqlStreamId.IdOriginal,
                              PageReadStatus.StreamNotFound,
                              start,
                              -1,
                              -1, 
                              -1,
                              direction,
                              true,
                              readNext),
                            StreamMeta.None);
                    }
                    var lastStreamVersion = reader.GetInt32(0);
                    var lastStreamPosition = reader.GetInt64(1);
                    var maxAge = reader.GetNullableInt32(2);
                    var maxCount = reader.GetNullableInt32(3);

                    await reader.NextResultAsync(cancellationToken).NotOnCapturedContext();
                    var messages = new List<(StreamMessage, int?)>();
                    while (await reader.ReadAsync(cancellationToken).NotOnCapturedContext())
                    {
                        if(messages.Count == count)
                        {
                            messages.Add(default);
                        }
                        else
                        {
                            var ordinal = 0;
                            var streamVersion1 = reader.GetInt32(ordinal++);
                            var position = reader.GetInt64(ordinal++);
                            var eventId = reader.GetGuid(ordinal++);
                            var created = reader.GetDateTime(ordinal++);
                            var type = reader.GetString(ordinal++);
                            var jsonMetadata = reader.GetString(ordinal++);

                            Func<CancellationToken, Task<string>> getJsonData;
                            if(prefetch)
                            {
                                var jsonData = reader.GetString(ordinal);
                                getJsonData = _ => Task.FromResult(jsonData);
                            }
                            else
                            {
                                getJsonData = ct => GetJsonData(sqlStreamId.Id, streamVersion1, ct);
                            }

                            var message = new StreamMessage(
                                sqlStreamId.IdOriginal,
                                eventId,
                                streamVersion1,
                                position,
                                created,
                                type,
                                jsonMetadata,
                                getJsonData);

                            messages.Add((message, maxAge));
                        }
                    }

                    var isEnd = true;
                    if(messages.Count == count + 1)
                    {
                        isEnd = false;
                        messages.RemoveAt(count);
                    }

                    var filteredMessages = FilterExpired(messages);
                    var meta = new StreamMeta(maxAge, maxCount);
                    return (new ReadStreamPage(
                        sqlStreamId.IdOriginal,
                        PageReadStatus.Success,
                        start,
                        getNextVersion(messages, lastStreamVersion),
                        lastStreamVersion,
                        lastStreamPosition,
                        direction,
                        isEnd,
                        readNext,
                        filteredMessages.ToArray()),
                        meta);
                }
            }
        }

        private async Task<string> GetJsonData(string streamId, int streamVersion, CancellationToken cancellationToken)
        {
            using(var connection = _createConnection())
            {
                await connection.OpenAsync(cancellationToken).NotOnCapturedContext();
                using(var command = new SqlCommand(_scripts.ReadMessageData, connection))
                {
                    command.Parameters.AddWithValue("streamId", streamId);
                    command.Parameters.AddWithValue("streamVersion", streamVersion);

                    var jsonData = (string)await command.ExecuteScalarAsync(cancellationToken).NotOnCapturedContext();
                    return jsonData;
                }
            }
        }
    }
}