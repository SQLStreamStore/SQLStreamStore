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
        protected override Task<ReadAllPage> ReadAllForwardsInternal(
            long fromPosition,
            int maxCount,
            bool prefetch,
            ReadNextAllPage readNext,
            CancellationToken cancellationToken)
        {
            GuardAgainstDisposed();
            
            return ReadAllInternal(
                fromPosition,
                maxCount,
                ReadDirection.Forward,
                prefetch,
                readNext,
                cancellationToken);
        }

        protected override Task<ReadAllPage> ReadAllBackwardsInternal(
            long fromPositionInclusive,
            int maxCount,
            bool prefetch,
            ReadNextAllPage readNext,
            CancellationToken cancellationToken)
        {
            GuardAgainstDisposed();
            
            return ReadAllInternal(
                fromPositionInclusive,
                maxCount,
                ReadDirection.Backward,
                prefetch,
                readNext,
                cancellationToken);
        }
        
        private async Task<ReadAllPage> ReadAllInternal(
            long fromPosition,
            int maxCount,
            ReadDirection direction,
            bool prefetch,
            ReadNextAllPage readNext,
            CancellationToken cancellationToken)
        {
            GuardAgainstDisposed();
            
            // We want to query one additional result to verify reaching the end of the line
            // -> Don't allow querying MaxValue
            maxCount = maxCount == int.MaxValue ? maxCount - 1 : maxCount;

            var readForward = direction == ReadDirection.Forward;
            var readCount = maxCount + 1;
            long readPosition = fromPosition;

            if(direction == ReadDirection.Backward)
            {
                readPosition = fromPosition == Position.End ? long.MaxValue : fromPosition;
            }
            
            using(var conn = await OpenConnection(cancellationToken))
            using(var cmd = _commandBuilder.ReadAll(conn, readPosition, readCount, readForward, prefetch))
            {
                await cmd.ExecuteNonQueryAsync(cancellationToken).NotOnCapturedContext();

                OracleDataAdapter adapter = new OracleDataAdapter(cmd);
                
                DataSet dsResult = new DataSet();
                adapter.Fill(dsResult, "allEvents", (OracleRefCursor) cmd.Parameters["allEvents"].Value);

                if(dsResult.Tables["allEvents"].Rows.Count == 0)
                {
                    if(direction == ReadDirection.Backward)
                    {
                        return new ReadAllPage(
                            Position.Start,
                            Position.Start,
                            true,
                            direction,
                            readNext,
                            Array.Empty<StreamMessage>());
                    }
                    
                    return new ReadAllPage(
                        fromPosition,
                        fromPosition,
                        true,
                        direction,
                        readNext,
                        Array.Empty<StreamMessage>());
                }

                var messages = dsResult
                    .Tables["allEvents"]
                    .AsEnumerable()
                    .Select(row =>
                    {
                        var msgStreamId = row.Field<string>("StreamId");
                        var msgId = Guid.Parse(row.Field<string>("ID"));
                        var msgStreamVersion = Convert.ToInt32(row.Field<long>("StreamVersion"));
                        var msgMaxAge = row.IsNull("MaxAge") ? (int?)null : Convert.ToInt32(row.Field<long>("MaxAge"));
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
                            msgGetJsonData = ct => GetJsonData(msgStreamId, msgId, ct);
                        }

                        var message = new StreamMessage(
                            msgStreamId,
                            msgId,
                            msgStreamVersion,
                            msgPosition,
                            msgCreated,
                            msgType,
                            msgJsonMeta,
                            msgGetJsonData);

                        return (message, maxAge: msgMaxAge);
                    })
                    .ToList();

                var isEnd = true;
                
                var lastPositionQueried = messages.Last().message.Position;
                
                if(messages.Count == maxCount + 1)
                {
                    isEnd = false;
                    messages.RemoveAt(maxCount);
                }
                
                var lastPosition = messages.Last().message.Position;
                var filteredMessages = FilterExpired(messages);

                long resultFromPosition = fromPosition;
                long resultNextPosition = lastPosition + 1;

                if(direction == ReadDirection.Backward)
                {
                    resultFromPosition = filteredMessages.Any() ? filteredMessages[0].Position : 0;
                    resultNextPosition = lastPositionQueried;
                }

                // if(direction == ReadDirection.Backward)
                // {
                //     resultFromPosition = filteredMessages.Any() ? filteredMessages[0].Position : 0;
                //     resultNextPosition = lastPosition.HasValue ? lastPosition.Value : Position.Start;
                // }
                // else
                // {
                //     resultFromPosition = fromPosition;
                //     resultNextPosition = lastPosition ?? -1;
                // }

                return new ReadAllPage(
                    resultFromPosition,
                    resultNextPosition,
                    isEnd,
                    direction,
                    readNext,
                    filteredMessages.ToArray()
                );
            }
        }
    }
}