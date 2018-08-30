namespace SqlStreamStore
{
    using System;
    using System.Data;
    using System.Data.SqlClient;
    using System.Linq;
    using System.Runtime.ExceptionServices;
    using System.Threading;
    using System.Threading.Tasks;
    using Microsoft.SqlServer.Server;
    using SqlStreamStore.Imports.Ensure.That;
    using SqlStreamStore.Streams;
    using SqlStreamStore.Infrastructure;

    public partial class MsSqlStreamStoreV3
    {
        private class MsSqlAppendResult
        {
            public readonly int? MaxCount;
            public readonly int CurrentVersion;
            public readonly long CurrentPosition;
            public readonly bool WasIdempotent;

            public MsSqlAppendResult(
                int? maxCount,
                int currentVersion,
                long currentPosition,
                bool wasIdempotent)
            {
                MaxCount = maxCount;
                CurrentVersion = currentVersion;
                CurrentPosition = currentPosition;
                WasIdempotent = wasIdempotent;
            }
        }

        protected override async Task<AppendResult> AppendToStreamInternal(
           string streamId,
           int expectedVersion,
           NewStreamMessage[] messages,
           CancellationToken cancellationToken)
        {
            Ensure.That(streamId, "streamId").IsNotNullOrWhiteSpace();
            Ensure.That(expectedVersion, "expectedVersion").IsGte(-3);
            Ensure.That(messages, "Messages").IsNotNull();
            GuardAgainstDisposed();

            MsSqlAppendResult result;
            using(var connection = _createConnection())
            {
                await connection.OpenAsync(cancellationToken).NotOnCapturedContext();
                var streamIdInfo = new StreamIdInfo(streamId);
                result = await AppendToStreamInternal(
                    connection,
                    null,
                    streamIdInfo.SqlStreamId,
                    expectedVersion,
                    messages,
                    cancellationToken);
            }

            if(result.MaxCount.HasValue && result.MaxCount.Value > 0)
            {
                await CheckStreamMaxCount(streamId, result.MaxCount, cancellationToken);
            }

            return new AppendResult(result.CurrentVersion, result.CurrentPosition);
        }

        private Task<MsSqlAppendResult> AppendToStreamInternal(
           SqlConnection connection,
           SqlTransaction transaction,
           SqlStreamId sqlStreamId,
           int expectedVersion,
           NewStreamMessage[] messages,
           CancellationToken cancellationToken)
        {
            GuardAgainstDisposed();

            return RetryOnDeadLock(() =>
            {
                if(expectedVersion == ExpectedVersion.Any)
                {
                    return AppendToStreamExpectedVersionAny(
                        connection,
                        transaction,
                        sqlStreamId,
                        messages,
                        cancellationToken);
                }
                if(expectedVersion == ExpectedVersion.NoStream)
                {
                    return AppendToStreamExpectedVersionNoStream(
                        connection,
                        transaction,
                        sqlStreamId,
                        messages,
                        cancellationToken);
                }
                if(expectedVersion == ExpectedVersion.EmptyStream)
                {
                    return AppendToStreamExpectedVersion(
                        connection,
                        transaction,
                        sqlStreamId,
                        -1,
                        messages,
                        cancellationToken);
                }
                return  AppendToStreamExpectedVersion(
                    connection,
                    transaction,
                    sqlStreamId,
                    expectedVersion,
                    messages,
                    cancellationToken);
            });
        }

        // Deadlocks appear to be a fact of life when there is high contention on a table regardless of 
        // transaction isolation settings. 
        private static async Task<T> RetryOnDeadLock<T>(Func<Task<T>> operation)
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
                catch(SqlException ex) when(ex.Number == 1205 || ex.Number == 1222) // Deadlock error codes;
                {
                    exception = ex;
                    retryCount++;
                }
            } while(retryCount < maxRetries);

            ExceptionDispatchInfo.Capture(exception).Throw();
            return default(T); // never actually run
        }

        private async Task<MsSqlAppendResult> AppendToStreamExpectedVersionAny(
            SqlConnection connection,
            SqlTransaction transaction,
            SqlStreamId sqlStreamId,
            NewStreamMessage[] messages,
            CancellationToken cancellationToken)
        {
            using(var command = new SqlCommand(_scripts.AppendStreamExpectedVersionAny, connection, transaction))
            {
                command.Parameters.Add(new SqlParameter("streamId", SqlDbType.Char, 42) { Value = sqlStreamId.Id });
                command.Parameters.AddWithValue("streamIdOriginal", sqlStreamId.IdOriginal);

                if (messages.Any())
                {
                    var sqlDataRecords = CreateSqlDataRecords(messages);
                    var eventsParam = CreateNewMessagesSqlParameter(sqlDataRecords);
                    command.Parameters.Add(eventsParam);
                    command.Parameters.AddWithValue("hasMessages", true);
                }
                else
                {
                    // Must use a null value for the table-valued param if there are no records
                    var eventsParam = CreateNewMessagesSqlParameter(null);
                    command.Parameters.Add(eventsParam);
                    command.Parameters.AddWithValue("hasMessages", false);
                }

                try
                {
                    using(var reader = await command
                        .ExecuteReaderAsync(cancellationToken)
                        .NotOnCapturedContext())
                    {
                        await reader.ReadAsync(cancellationToken).NotOnCapturedContext();

                        var currentVersion = reader.GetInt32(0);
                        var currentPosition = reader.GetInt64(1);
                        var maxCount = reader.GetNullableInt32(2);

                        return new MsSqlAppendResult(maxCount, currentVersion, currentPosition, false);
                    }
                }
                
                // Check for unique constraint violation on 
                // https://technet.microsoft.com/en-us/library/aa258747%28v=sql.80%29.aspx
                catch(SqlException ex)
                    when(ex.IsUniqueConstraintViolationOnIndex("IX_Messages_StreamIdInternal_Id"))
                {
                    var streamVersion = await GetStreamVersionOfMessageId(
                        connection,
                        transaction,
                        sqlStreamId,
                        messages[0].MessageId,
                        cancellationToken);

                    // Idempotency handling. Check if the Messages have already been written.
                    var (page, meta) = await ReadStreamInternal(
                        sqlStreamId,
                        streamVersion,
                        messages.Length,
                        ReadDirection.Forward,
                        false,
                        null,
                        connection,
                        transaction,
                        cancellationToken)
                        .NotOnCapturedContext();

                    if(messages.Length > page.Messages.Length)
                    {
                        throw new WrongExpectedVersionException(
                            ErrorMessages.AppendFailedWrongExpectedVersion(sqlStreamId.IdOriginal, ExpectedVersion.Any),
                            ex);
                    }

                    for(int i = 0; i < Math.Min(messages.Length, page.Messages.Length); i++)
                    {
                        if(messages[i].MessageId != page.Messages[i].MessageId)
                        {
                            throw new WrongExpectedVersionException(
                                ErrorMessages.AppendFailedWrongExpectedVersion(sqlStreamId.IdOriginal, ExpectedVersion.Any),
                                ex);
                        }
                    }

                    return new MsSqlAppendResult(
                        meta.MaxCount,
                        page.LastStreamVersion,
                        page.LastStreamPosition,
                        true);
                }
                catch(SqlException ex) when(ex.IsUniqueConstraintViolation())
                {
                    throw new WrongExpectedVersionException(
                        ErrorMessages.AppendFailedWrongExpectedVersion(sqlStreamId.IdOriginal, ExpectedVersion.Any),
                        ex);
                }
            }
        }

        private async Task<MsSqlAppendResult> AppendToStreamExpectedVersionNoStream(
            SqlConnection connection,
            SqlTransaction transaction,
            SqlStreamId sqlStreamId,
            NewStreamMessage[] messages,
            CancellationToken cancellationToken)
        {
            using(var command = new SqlCommand(_scripts.AppendStreamExpectedVersionNoStream, connection, transaction))
            {
                command.Parameters.Add(new SqlParameter("streamId", SqlDbType.Char, 42) { Value = sqlStreamId.Id });
                command.Parameters.AddWithValue("streamIdOriginal", sqlStreamId.IdOriginal);

                if(messages.Length != 0)
                {
                    var sqlDataRecords = CreateSqlDataRecords(messages);
                    var eventsParam = CreateNewMessagesSqlParameter(sqlDataRecords);
                    command.Parameters.Add(eventsParam);
                    command.Parameters.AddWithValue("hasMessages", true);
                }
                else
                {
                    // Must use a null value for the table-valued param if there are no records
                    var eventsParam = CreateNewMessagesSqlParameter(null);
                    command.Parameters.Add(eventsParam);
                    command.Parameters.AddWithValue("hasMessages", false);
                }

                try
                {
                    using(var reader = await command
                        .ExecuteReaderAsync(cancellationToken)
                        .NotOnCapturedContext())
                    {
                        await reader.ReadAsync(cancellationToken).NotOnCapturedContext();

                        var currentVersion = reader.GetInt32(0);
                        var currentPosition = reader.GetInt64(1);
                        var maxCount = reader.GetNullableInt32(2);

                        return new MsSqlAppendResult(maxCount, currentVersion, currentPosition, false);
                    }
                }
                catch(SqlException ex)
                {
                    // Check for unique constraint violation on 
                    // https://technet.microsoft.com/en-us/library/aa258747%28v=sql.80%29.aspx
                    if(ex.IsUniqueConstraintViolationOnIndex("IX_Streams_Id"))
                    {
                        // Idempotency handling. Check if the Messages have already been written.
                        var (page, meta) = await ReadStreamInternal(
                                sqlStreamId,
                                StreamVersion.Start,
                                messages.Length,
                                ReadDirection.Forward,
                                false,
                                null,
                                connection,
                                transaction,
                                cancellationToken)
                            .NotOnCapturedContext();

                        if(messages.Length > page.Messages.Length)
                        {
                            throw new WrongExpectedVersionException(
                                ErrorMessages.AppendFailedWrongExpectedVersion(
                                    sqlStreamId.IdOriginal,
                                    ExpectedVersion.NoStream),
                                ex);
                        }

                        for(int i = 0; i < Math.Min(messages.Length, page.Messages.Length); i++)
                        {
                            if(messages[i].MessageId != page.Messages[i].MessageId)
                            {
                                throw new WrongExpectedVersionException(
                                    ErrorMessages.AppendFailedWrongExpectedVersion(sqlStreamId.IdOriginal,
                                        ExpectedVersion.NoStream),
                                    ex);
                            }
                        }

                        return new MsSqlAppendResult(
                            meta.MaxCount,
                            page.LastStreamVersion,
                            page.LastStreamPosition,
                            true);
                    }

                    if(ex.IsUniqueConstraintViolation())
                    {
                        throw new WrongExpectedVersionException(
                            ErrorMessages.AppendFailedWrongExpectedVersion(sqlStreamId.IdOriginal,
                                ExpectedVersion.NoStream),
                            ex);
                    }

                    throw;
                }
            }
        }

        private async Task<MsSqlAppendResult> AppendToStreamExpectedVersion(
            SqlConnection connection,
            SqlTransaction transaction,
            SqlStreamId sqlStreamId,
            int expectedVersion,
            NewStreamMessage[] messages,
            CancellationToken cancellationToken)
        {
            var sqlDataRecords = CreateSqlDataRecords(messages);

            using(var command = new SqlCommand(_scripts.AppendStreamExpectedVersion, connection, transaction))
            {
                command.Parameters.Add(new SqlParameter("streamId", SqlDbType.Char, 42) { Value = sqlStreamId.Id });
                command.Parameters.AddWithValue("expectedStreamVersion", expectedVersion);
                var eventsParam = CreateNewMessagesSqlParameter(sqlDataRecords);
                command.Parameters.Add(eventsParam);

                try
                {
                    using (var reader = await command
                        .ExecuteReaderAsync(cancellationToken)
                        .NotOnCapturedContext())
                    {
                        await reader.ReadAsync(cancellationToken).NotOnCapturedContext();

                        var currentVersion = reader.GetInt32(0);
                        var currentPosition = reader.GetInt64(1);
                        var maxCount = reader.GetNullableInt32(2);

                        return new MsSqlAppendResult(maxCount, currentVersion, currentPosition, false);
                    }
                }
                catch(SqlException ex)
                {
                    if(ex.Errors.Count == 1)
                    {
                        var sqlError = ex.Errors[0];
                        if(sqlError.Message == "WrongExpectedVersion")
                        {
                            // Idempotency handling. Check if the Messages have already been written.

                            var(page, meta) = await ReadStreamInternal(
                                sqlStreamId,
                                expectedVersion + 1,
                                // when reading for already written Messages, it's from the one after the expected
                                messages.Length,
                                ReadDirection.Forward,
                                false,
                                null,
                                connection,
                                transaction,
                                cancellationToken);

                            if(messages.Length > page.Messages.Length)
                            {
                                throw new WrongExpectedVersionException(
                                    ErrorMessages.AppendFailedWrongExpectedVersion(sqlStreamId.IdOriginal, expectedVersion),
                                    ex);
                            }

                            // Iterate all messages an check to see if all message ids match
                            for(int i = 0; i < Math.Min(messages.Length, page.Messages.Length); i++)
                            {
                                if(messages[i].MessageId != page.Messages[i].MessageId)
                                {
                                    throw new WrongExpectedVersionException(
                                        ErrorMessages.AppendFailedWrongExpectedVersion(sqlStreamId.IdOriginal, expectedVersion),
                                        ex);
                                }
                            }

                            return new MsSqlAppendResult(
                                meta.MaxCount,
                                page.LastStreamVersion,
                                page.LastStreamPosition,
                                true);
                        }
                    }
                    if(ex.IsUniqueConstraintViolation())
                    {
                        throw new WrongExpectedVersionException(
                            ErrorMessages.AppendFailedWrongExpectedVersion(sqlStreamId.IdOriginal, expectedVersion),
                            ex);
                    }
                    throw;
                }
            }
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

        private async Task<int> GetStreamVersionOfMessageId(
            SqlConnection connection,
            SqlTransaction transaction,
            SqlStreamId sqlStreamId,
            Guid messageId,
            CancellationToken cancellationToken)
        {
            using(var command = new SqlCommand(_scripts.GetStreamVersionOfMessageId, connection, transaction))
            {
                command.Parameters.Add(new SqlParameter("streamId", SqlDbType.Char, 42) { Value = sqlStreamId.Id });
                command.Parameters.AddWithValue("messageId", messageId);

                var result = await command.ExecuteScalarAsync(cancellationToken)
                    .NotOnCapturedContext();

                return (int) result;
            }
        }

        private SqlDataRecord[] CreateSqlDataRecords(NewStreamMessage[] messages)
        {
            var dateTime = GetUtcNow();
            var sqlDataRecords = messages.Select(message =>
            {
                var record = new SqlDataRecord(_appendToStreamSqlMetadata);
                record.SetGuid(1, message.MessageId);
                record.SetDateTime(2, dateTime);
                record.SetString(3, message.Type);
                record.SetString(4, message.JsonData);
                record.SetString(5, message.JsonMetadata);
                return record;
            }).ToArray();
            return sqlDataRecords;
        }

        private SqlParameter CreateNewMessagesSqlParameter(SqlDataRecord[] sqlDataRecords)
        {
            var eventsParam = new SqlParameter("newMessages", SqlDbType.Structured)
            {
                TypeName = $"{_scripts.Schema}.NewStreamMessages",
                Value = sqlDataRecords
            };
            return eventsParam;
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