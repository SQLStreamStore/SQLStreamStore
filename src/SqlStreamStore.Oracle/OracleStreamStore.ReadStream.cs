namespace SqlStreamStore.Oracle
{
    using System;
    using System.Data;
    using System.Linq;
    using System.Threading;
    using System.Threading.Tasks;
    using global::Oracle.ManagedDataAccess.Client;
    using global::Oracle.ManagedDataAccess.Types;
    using SqlStreamStore.Infrastructure;
    using SqlStreamStore.Streams;

    partial class OracleStreamStore
    {
        protected override Task<ReadStreamPage> ReadStreamForwardsInternal(
            string streamId,
            int start,
            int count,
            bool prefetch,
            ReadNextStreamPage readNext,
            CancellationToken cancellationToken)
        {
            return ReadStreamInternal(
                new OracleStreamId(streamId),
                start,
                count,
                ReadDirection.Forward,
                prefetch,
                readNext,
                cancellationToken);
        }

        protected override Task<ReadStreamPage> ReadStreamBackwardsInternal(
            string streamId,
            int fromVersionInclusive,
            int count,
            bool prefetch,
            ReadNextStreamPage readNext,
            CancellationToken cancellationToken)
        {
            return ReadStreamInternal(
                new OracleStreamId(streamId),
                fromVersionInclusive,
                count,
                ReadDirection.Backward,
                prefetch,
                readNext,
                cancellationToken);
        }
        
        private async Task<ReadStreamPage> ReadStreamInternal(
            OracleStreamId streamId,
            int start,
            int count,
            ReadDirection direction,
            bool prefetch,
            ReadNextStreamPage readNext,
            CancellationToken cancellationToken)
        {
            // If the count is int.MaxValue, TSql will see it as a negative number. 
            // Users shouldn't be using int.MaxValue in the first place anyway.
            count = count == int.MaxValue ? count - 1 : count;

            // To read backwards from end, need to use int MaxValue
            var streamVersion = start == StreamVersion.End ? int.MaxValue : start;
            
            using(var conn = await OpenConnection(cancellationToken))
            using(var cmd = _commandBuilder.ReadStream(conn, streamId, count + 1, streamVersion, direction == ReadDirection.Forward, prefetch))
            {
                await cmd.ExecuteNonQueryAsync(cancellationToken).NotOnCapturedContext();

                OracleDataAdapter adapter = new OracleDataAdapter(cmd);
                
                DataSet dsResult = new DataSet();
                adapter.Fill(dsResult, "streamInfo", (OracleRefCursor) cmd.Parameters["streamInfo"].Value);
                adapter.Fill(dsResult, "streamEvents", (OracleRefCursor) cmd.Parameters["streamEvents"].Value);

                if(dsResult.Tables["streamInfo"].Rows.Count == 0)
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
                
                var streamMetaRow = dsResult
                    .Tables["streamInfo"]
                    .Rows[0];

                var maxAge = streamMetaRow.IsNull("MaxAge") ? (int?)null : Convert.ToInt32(streamMetaRow.Field<long>("MaxAge"));
                
                var lastStreamVersion =  Convert.ToInt32(streamMetaRow.Field<long>("Version"));
                var latestStreamPosition = Convert.ToInt64(streamMetaRow.Field<decimal>("Position"));
                
                var messages = dsResult
                    .Tables["streamEvents"]
                    .AsEnumerable()
                    .Select(row =>
                    {
                        var msgId = Guid.Parse(row.Field<string>("ID"));
                        var msgStreamVersion = Convert.ToInt32(row.Field<long>("StreamVersion"));
                        var msgPosition = Convert.ToInt64(row.Field<Decimal>("Position"));
                        var msgCreated = row.Field<DateTime>("Created");
                        var msgType = row.Field<string>("Type");
                        var msgJsonMeta = row.Field<string>("JsonMeta");

                        Func<CancellationToken, Task<string>> msgGetJsonData = token => Task.FromResult<string>(null);
                        if(prefetch)
                        {
                            var msgJsonData = row.Field<string>("JsonData");
                            msgGetJsonData = token => Task.FromResult(msgJsonData);
                        }
                        else
                        {
                            msgGetJsonData = ct => GetJsonData(streamId.Id, msgId, ct);
                        }

                        var message = new StreamMessage(
                            streamId.IdOriginal,
                            msgId,
                            msgStreamVersion,
                            msgPosition,
                            msgCreated,
                            msgType,
                            msgJsonMeta,
                            msgGetJsonData);

                        return (message, maxAge);
                    })
                    .ToList();

                var isEnd = true;
                if(messages.Count == count + 1)
                {
                    isEnd = false;
                    messages.RemoveAt(count);
                }

                var nextVersion = messages.Any() 
                    ? messages.Last().message.StreamVersion + 
                      (direction == ReadDirection.Forward 
                          ? +1 
                          : -1)
                    : (direction == ReadDirection.Forward) 
                        ? lastStreamVersion + 1 
                        : -1;
                
                var filteredMessages = FilterExpired(messages);
                
                return new ReadStreamPage(
                    streamId.IdOriginal, 
                    PageReadStatus.Success, 
                    start, 
                    nextVersion,
                    lastStreamVersion,
                    latestStreamPosition,
                    direction,
                    isEnd,
                    readNext,
                    filteredMessages.ToArray()
                );
                
            }
        }
        
        private async Task<string> GetJsonData(string streamId, Guid messageId, CancellationToken cancellationToken)
        {
            using(var conn = await OpenConnection(cancellationToken))
            using(var cmd = _commandBuilder.ReadMessageData(conn, streamId, messageId))
            {
                var data = await cmd.ExecuteScalarAsync(cancellationToken).NotOnCapturedContext();

                return data as string;
            }
        }


        
    }
}