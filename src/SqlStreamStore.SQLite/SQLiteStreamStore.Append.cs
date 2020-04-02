namespace SqlStreamStore
{
    using System;
    using System.Data.SQLite;
    using System.Runtime.ExceptionServices;
    using System.Threading;
    using System.Threading.Tasks;
    using SqlStreamStore.Imports.Ensure.That;
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
            Ensure.That(streamId, nameof(streamId)).IsNotNullOrWhiteSpace();
            Ensure.That(expectedVersion, nameof(expectedVersion)).IsGte(ExpectedVersion.NoStream);
            Ensure.That(messages, nameof(messages)).IsNotNull();
            GuardAgainstDisposed();

            SQLiteAppendResult result;
            using (var connection = _createConnection())
            {
                await connection.OpenAsync(cancellationToken).NotOnCapturedContext();
                var streamIdInfo = new StreamIdInfo(streamId);
                result = await AppendToStreamInternal(connection, null, streamIdInfo.SQLiteStreamId, expectedVersion, messages, cancellationToken);
            }

            if (result.MaxCount.HasValue)
            {
                await CheckStreamMaxCount(streamId, result.MaxCount.Value, cancellationToken);
            }

            return new AppendResult(result.CurrentVersion, result.CurrentPosition);
        }

        private Task<SQLiteAppendResult> AppendToStreamInternal(SQLiteConnection connection, SQLiteTransaction transaction, SQLiteStreamId streamId, int expectedVersion, NewStreamMessage[] messages, CancellationToken cancellationToken)
        {
            GuardAgainstDisposed();

            return this.RetryOnDeadLock(() => {
                switch (expectedVersion)
                {
                    case ExpectedVersion.Any:
                        return connection.AppendToStreamExpectedVersionAny(
                            transaction,
                            streamId,
                            messages,
                            GetUtcNow,
                            cancellationToken);
                    case ExpectedVersion.NoStream:
                        return connection.AppendToStreamExpectedVersionNoStream(
                            transaction,
                            streamId,
                            messages,
                            GetUtcNow,
                            cancellationToken);
                    case ExpectedVersion.EmptyStream:
                        return connection.AppendToStreamExpectedVersion(
                            transaction,
                            streamId,
                            expectedVersion,
                            messages,
                            GetUtcNow,
                            cancellationToken);
                }

                return connection.AppendToStreamExpectedVersion(
                    transaction, 
                    streamId, 
                    expectedVersion, 
                    messages, 
                    GetUtcNow, 
                    cancellationToken);
            });
        }

        private async Task CheckStreamMaxCount(string streamId, int? maxCount, CancellationToken cancellationToken)
        {
            if (maxCount.HasValue)
            {
                var count = await GetStreamMessageCount(streamId, cancellationToken);
                if (count > maxCount.Value)
                {
                    int toPurge = count - maxCount.Value;

                    var streamMessagesPage = await ReadStreamForwardsInternal(streamId, StreamVersion.Start,
                        toPurge, false, null, cancellationToken);

                    if (streamMessagesPage.Status == PageReadStatus.Success)
                    {
                        foreach (var message in streamMessagesPage.Messages)
                        {
                            await DeleteEventInternal(streamId, message.MessageId, cancellationToken);
                        }
                    }
                }
            }
        }

        private async Task<int> GetStreamMessageCount(
            string streamId,
            CancellationToken cancellationToken = default)
        {
            GuardAgainstDisposed();

            using(var connection = _createConnection())
            {
                await connection.OpenAsync(cancellationToken).NotOnCapturedContext();

                using(var command = new SQLiteCommand(_scripts.GetStreamMessageCount, connection))
                {
                    var streamIdInfo = new StreamIdInfo(streamId);
                    command.Parameters.Add(new SQLiteParameter("@streamId", streamId));

                    var result = await command
                        .ExecuteScalarAsync(cancellationToken)
                        .NotOnCapturedContext();

                    return (int) result;
                }
            }
        }

        // Deadlocks appear to be a fact of life when there is high contention on a table regardless of
        // transaction isolation settings.
        private async Task<T> RetryOnDeadLock<T>(Func<Task<T>> operation)
        {
            int maxRetries = 2; //TODO too much? too little? configurable?
            Exception exception;

            int retryCount = 0;
            do
            {
                try
                {
                    return await operation();
                }
                catch(SQLiteException ex)
                {
                    exception = ex;
                    retryCount++;
                }
            } while(retryCount < maxRetries);

            ExceptionDispatchInfo.Capture(exception).Throw();
            return default(T); // never actually run
        }

        internal class StreamMeta
        {
            public static readonly StreamMeta None = new StreamMeta(null, null);

            public StreamMeta(int? maxCount, int? maxAge)
            {
                MaxCount = maxCount;
                MaxAge = maxAge;
            }

            public int? MaxCount { get; }

            public int? MaxAge { get; }
        }
    }
}