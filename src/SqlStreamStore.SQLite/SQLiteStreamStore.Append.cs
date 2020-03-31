namespace SqlStreamStore
{
    using System;
    using System.Data.SQLite;
    using System.Runtime.ExceptionServices;
    using System.Threading;
    using System.Threading.Tasks;
    using SqlStreamStore.Infrastructure;
    using SqlStreamStore.Streams;

    partial class SQLiteStreamStore
    {
        protected override async Task<AppendResult> AppendToStreamInternal(
            string streamId, 
            int expectedVersion, 
            NewStreamMessage[] messages, 
            CancellationToken cancellationToken)
        {
            int maxRetries = 2;
            Exception exception;

            int retryCount = 0;
            do
            {
                try
                {
                    AppendResult result = default;
                    var streamIdInfo = new StreamIdInfo(streamId);

                    using(var connection = await OpenConnection(cancellationToken))
                    using(var transaction = connection.BeginTransaction())
                    using(var command = connection.CreateCommand())
                    {
                        try
                        {
                            using (var reader = await command
                                .ExecuteReaderAsync(cancellationToken)
                                .NotOnCapturedContext())
                            {
                                await reader.ReadAsync(cancellationToken).NotOnCapturedContext();

                                result = new AppendResult(reader.GetInt32(0), reader.GetInt64(1));
                            }

                            transaction.Commit();
                        }
                        catch (SQLiteException ex)
                        {
                            transaction.Rollback();

                            //TODO: throw new WrongExpectedVersionException();
                        }
                    }

                    if(_settings.ScavengeAsynchronously)
                    {
#pragma warning disable CS4014 // Because this call is not awaited, execution of the current method continues before the call is completed
                        Task.Run(() => TryScavange(streamIdInfo, cancellationToken), cancellationToken);
#pragma warning restore CS4014 // Because this call is not awaited, execution of the current method continues before the call is completed
                    }
                    else
                    {
                        await TryScavange(streamIdInfo, cancellationToken).NotOnCapturedContext();
                    }

                    return result;
                }
                catch (SQLiteException ex)
                {
                    exception = ex;
                    retryCount++;
                }
            } while(retryCount < maxRetries);

            ExceptionDispatchInfo.Capture(exception).Throw();
            return default;
        }
    }
}