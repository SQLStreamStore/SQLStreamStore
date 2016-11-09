namespace SqlStreamStore
{
    using System;
    using System.Data;
    using System.Data.SqlClient;
    using System.Linq;
    using System.Runtime.ExceptionServices;
    using System.Threading;
    using System.Threading.Tasks;
    using EnsureThat;
    using Microsoft.SqlServer.Server;
    using SqlStreamStore.Streams;
    using SqlStreamStore.Infrastructure;
    using StreamStoreStore.Json;

    public partial class MsSqlStreamStore
    {
        private static readonly Tuple<int?, int> NullAppendResult = new Tuple<int?, int>(null, -1);

        protected override async Task<AppendResult> AppendToStreamInternal(
           string streamId,
           int expectedVersion,
           NewStreamMessage[] messages,
           CancellationToken cancellationToken)
        {
            Ensure.That(streamId, "streamId").IsNotNullOrWhiteSpace();
            Ensure.That(expectedVersion, "expectedVersion").IsGte(-2);
            Ensure.That(messages, "Messages").IsNotNull();
            GuardAgainstDisposed();

            Tuple<int?,int> result;
            using(var connection = _createConnection())
            {
                await connection.OpenAsync(cancellationToken).NotOnCapturedContext();
                var streamIdInfo = new StreamIdInfo(streamId);
                result = await AppendToStreamInternal(connection, null, streamIdInfo.SqlStreamId, expectedVersion,
                    messages, cancellationToken);
            }

            if(result.Item1 != null)
            {
                await CheckStreamMaxCount(streamId, result.Item1, cancellationToken);
            }

            return new AppendResult(result.Item2);
        }

        private async Task<Tuple<int?, int>> AppendToStreamInternal(
           SqlConnection connection,
           SqlTransaction transaction,
           SqlStreamId sqlStreamId,
           int expectedVersion,
           NewStreamMessage[] messages,
           CancellationToken cancellationToken)
        {
            GuardAgainstDisposed();

            if (expectedVersion == ExpectedVersion.Any)
            {
                // Deadlock can occur when creating the stream for the first time and multiple threads attempting to 
                // append with expected version any.
                return await RetryOnDeadLock(() => AppendToStreamExpectedVersionAny(
                    connection,
                    transaction,
                    sqlStreamId,
                    messages,
                    cancellationToken));
            }
            if(expectedVersion == ExpectedVersion.NoStream)
            {
                return await AppendToStreamExpectedVersionNoStream(
                    connection,
                    transaction,
                    sqlStreamId,
                    messages,
                    cancellationToken);
            }
            return await AppendToStreamExpectedVersion(
                connection,
                transaction,
                sqlStreamId,
                expectedVersion,
                messages,
                cancellationToken);
        }

        private async Task<T> RetryOnDeadLock<T>(Func<Task<T>> operation, int maxRetries = 2)
        {
            Exception exception;

            int retryCount = 0;
            do
            {
                try
                {
                    return await operation();
                }
                catch(SqlException ex) when(ex.Number == 1205 || ex.Number == 1222) // Deadlock error code;
                {
                    exception = ex;
                    retryCount++;
                }
            } while(retryCount < maxRetries);

            ExceptionDispatchInfo.Capture(exception).Throw();
            return default(T); // never actually run
        }

        private async Task<Tuple<int?, int>> AppendToStreamExpectedVersionAny(
            SqlConnection connection,
            SqlTransaction transaction,
            SqlStreamId sqlStreamId,
            NewStreamMessage[] messages,
            CancellationToken cancellationToken)
        {
            using(var command = new SqlCommand(_scripts.AppendStreamExpectedVersionAny, connection, transaction))
            {
                command.Parameters.AddWithValue("streamId", sqlStreamId.Id);
                command.Parameters.AddWithValue("streamIdOriginal", sqlStreamId.IdOriginal);

                if (messages.Any())
                {
                    var sqlDataRecords = CreateSqlDataRecords(messages);
                    var eventsParam = CreateNewMessagesSqlParameter(sqlDataRecords);
                    command.Parameters.Add(eventsParam);
                }
                else
                {
                    // Must use a null value for the table-valued param if there are no records
                    var eventsParam = CreateNewMessagesSqlParameter(null);
                    command.Parameters.Add(eventsParam);
                }

                try
                {
                    using(var reader = await command
                        .ExecuteReaderAsync(cancellationToken)
                        .NotOnCapturedContext())
                    {
                        await reader.ReadAsync(cancellationToken).NotOnCapturedContext();
                        var currentVersion = reader.GetInt32(0);
                        int? maxCount = null;

                        await reader.NextResultAsync(cancellationToken);
                        if(await reader.ReadAsync(cancellationToken).NotOnCapturedContext())
                        {
                            var jsonData = reader.GetString(0);
                            var metadataMessage = SimpleJson.DeserializeObject<MetadataMessage>(jsonData);
                            maxCount = metadataMessage.MaxCount;
                        }

                        return new Tuple<int?, int>(maxCount, currentVersion);
                    }
                }
                
                // Check for unique constraint violation on 
                // https://technet.microsoft.com/en-us/library/aa258747%28v=sql.80%29.aspx
                catch(SqlException ex)
                    when(ex.IsUniqueConstraintViolationOnIndex("IX_Messages_StreamIdInternal_Id"))
                {
                    // Idempotency handling. Check if the Messages have already been written.
                    var page = await ReadStreamInternal(
                        sqlStreamId,
                        StreamVersion.Start,
                        messages.Length,
                        ReadDirection.Forward,
                        false,
                        null,
                        connection,
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
                }
                catch(SqlException ex) when(ex.IsUniqueConstraintViolation())
                {
                    throw new WrongExpectedVersionException(
                        ErrorMessages.AppendFailedWrongExpectedVersion(sqlStreamId.IdOriginal, ExpectedVersion.Any),
                        ex);
                }
                return NullAppendResult;
            }
        }

        private async Task<Tuple<int?, int>> AppendToStreamExpectedVersionNoStream(
            SqlConnection connection,
            SqlTransaction transaction,
            SqlStreamId sqlStreamId,
            NewStreamMessage[] messages,
            CancellationToken cancellationToken)
        {
            using(var command = new SqlCommand(_scripts.AppendStreamExpectedVersionNoStream, connection, transaction))
            {
                command.Parameters.AddWithValue("streamId", sqlStreamId.Id);
                command.Parameters.AddWithValue("streamIdOriginal", sqlStreamId.IdOriginal);

                if(messages.Any())
                {
                    var sqlDataRecords = CreateSqlDataRecords(messages);
                    var eventsParam = CreateNewMessagesSqlParameter(sqlDataRecords);
                    command.Parameters.Add(eventsParam);
                }
                else
                {
                    // Must use a null value for the table-valued param if there are no records
                    var eventsParam = CreateNewMessagesSqlParameter(null);
                    command.Parameters.Add(eventsParam);
                }

                try
                {
                    using(var reader = await command
                        .ExecuteReaderAsync(cancellationToken)
                        .NotOnCapturedContext())
                    {
                        await reader.ReadAsync(cancellationToken).NotOnCapturedContext();
                        var currentVersion = reader.GetInt32(0);
                        int? maxCount = null;

                        if (await reader.ReadAsync(cancellationToken).NotOnCapturedContext())
                        {
                            var jsonData = reader.GetString(0);
                            var metadataMessage = SimpleJson.DeserializeObject<MetadataMessage>(jsonData);
                            maxCount = metadataMessage.MaxCount;
                        }

                        return new Tuple<int?, int>(maxCount, currentVersion);

                    }
                }
                catch(SqlException ex)
                {
                    // Check for unique constraint violation on 
                    // https://technet.microsoft.com/en-us/library/aa258747%28v=sql.80%29.aspx
                    if(ex.IsUniqueConstraintViolationOnIndex("IX_Streams_Id"))
                    {
                        // Idempotency handling. Check if the Messages have already been written.
                        var page = await ReadStreamInternal(
                                sqlStreamId,
                                StreamVersion.Start,
                                messages.Length,
                                ReadDirection.Forward,
                                false,
                                null,
                                connection,
                                cancellationToken)
                            .NotOnCapturedContext();

                        if(messages.Length > page.Messages.Length)
                        {
                            throw new WrongExpectedVersionException(
                                ErrorMessages.AppendFailedWrongExpectedVersion(sqlStreamId.IdOriginal,
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

                        return NullAppendResult;
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
                return NullAppendResult;
            }
        }

        private async Task<Tuple<int?, int>> AppendToStreamExpectedVersion(
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
                command.Parameters.AddWithValue("streamId", sqlStreamId.Id);
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
                        int? maxCount = null;

                        await reader.NextResultAsync(cancellationToken);
                        if (await reader.ReadAsync(cancellationToken).NotOnCapturedContext())
                        {
                            var jsonData = reader.GetString(0);
                            var metadataMessage = SimpleJson.DeserializeObject<MetadataMessage>(jsonData);
                            maxCount = metadataMessage.MaxCount;

                        }

                        return new Tuple<int?, int>(maxCount, currentVersion);
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

                            var page = await ReadStreamInternal(
                                sqlStreamId,
                                expectedVersion + 1,
                                // when reading for already written Messages, it's from the one after the expected
                                messages.Length,
                                ReadDirection.Forward,
                                false,
                                null,
                                connection,
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

                            return NullAppendResult;
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
                return NullAppendResult;
            }
        }

        private async Task CheckStreamMaxCount(string streamId, int? maxCount, CancellationToken cancellationToken)
        {
            if (maxCount.HasValue)
            {
                var count = await GetmessageCount(streamId, cancellationToken);
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
    }
}