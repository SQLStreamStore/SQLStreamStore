namespace SqlStreamStore
{
    using System;
    using System.Collections.Generic;
    using System.Data.SqlClient;
    using System.Linq;
    using System.Threading;
    using System.Threading.Tasks;
    using SqlStreamStore.Streams;
    using SqlStreamStore.Infrastructure;

    public partial class MsSqlStreamStore
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
                return await ReadStreamInternal(streamIdInfo.SqlStreamId, start, count, ReadDirection.Forward,
                    prefetch, readNext, connection, cancellationToken);
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
                return await ReadStreamInternal(streamIdInfo.SqlStreamId, start, count, ReadDirection.Backward,
                    prefetch, readNext, connection, cancellationToken);
            }
        }

        private async Task<ReadStreamPage> ReadStreamInternal(
            SqlStreamId sqlStreamId,
            int start,
            int count,
            ReadDirection direction,
            bool prefetch,
            ReadNextStreamPage readNext,
            SqlConnection connection, CancellationToken cancellationToken)
        {
            // If the count is int.MaxValue, TSql will see it as a negative number. 
            // Users shouldn't be using int.MaxValue in the first place anyway.
            count = count == int.MaxValue ? count - 1 : count;

            // To read backwards from end, need to use int MaxValue
            var streamVersion = start == StreamVersion.End ? int.MaxValue : start;
            string commandText;
            Func<List<StreamMessage>, int, int> getNextVersion;
            if(direction == ReadDirection.Forward)
            {
                commandText = prefetch ? _scripts.ReadStreamForwardWithData : _scripts.ReadStreamForward;
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
                commandText = _scripts.ReadStreamBackward;
                getNextVersion = (events, lastVersion) =>
                {
                    if (events.Any())
                    {
                        return events.Last().StreamVersion - 1;
                    }
                    return -1;
                };
            }

            using(var command = new SqlCommand(commandText, connection))
            {
                command.Parameters.AddWithValue("streamId", sqlStreamId.Id);
                command.Parameters.AddWithValue("count", count + 1); //Read extra row to see if at end or not
                command.Parameters.AddWithValue("streamVersion", streamVersion);

                using(var reader = await command.ExecuteReaderAsync(cancellationToken).NotOnCapturedContext())
                {
                    await reader.ReadAsync(cancellationToken).NotOnCapturedContext();
                    if(reader.IsDBNull(0))
                    {
                        return new ReadStreamPage(
                              sqlStreamId.IdOriginal,
                              PageReadStatus.StreamNotFound,
                              start,
                              -1,
                              -1,
                              direction,
                              true,
                              StreamMessage.EmptyArray,
                              readNext);
                    }
                    var lastStreamVersion = reader.GetInt32(0);

                    await reader.NextResultAsync(cancellationToken).NotOnCapturedContext();
                    var messages = new List<StreamMessage>();
                    while (await reader.ReadAsync(cancellationToken).NotOnCapturedContext())
                    {
                        var streamVersion1 = reader.GetInt32(0);
                        var ordinal = reader.GetInt64(1);
                        var eventId = reader.GetGuid(2);
                        var created = reader.GetDateTime(3);
                        var type = reader.GetString(4);
                        var jsonMetadata = reader.GetString(5);

                        var getJsonData = prefetch
                            ? (Func<CancellationToken, Task<string>>) (_ => Task.FromResult(reader.GetString(5)))
                            : (ct => GetJsonData(sqlStreamId.Id, streamVersion1, ct));

                        var message = new StreamMessage(
                            sqlStreamId.IdOriginal,
                            eventId,
                            streamVersion1,
                            ordinal,
                            created,
                            type,
                            getJsonData,
                            jsonMetadata);

                        messages.Add(message);
                    }

                    var isEnd = true;
                    if(messages.Count == count + 1)
                    {
                        isEnd = false;
                        messages.RemoveAt(count);
                    }

                    return new ReadStreamPage(
                        sqlStreamId.IdOriginal,
                        PageReadStatus.Success,
                        start,
                        getNextVersion(messages, lastStreamVersion),
                        lastStreamVersion,
                        direction,
                        isEnd,
                        messages.ToArray(),
                        readNext);
                }
            }
        }

        private async Task<string> GetJsonData(string streamId, int streamVersion, CancellationToken cancellationToken)
        {
            using(var connection = _createConnection())
            {
                await connection.OpenAsync(cancellationToken).NotOnCapturedContext();
                using(var command = new SqlCommand(_scripts.ReadStreamMessage, connection))
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