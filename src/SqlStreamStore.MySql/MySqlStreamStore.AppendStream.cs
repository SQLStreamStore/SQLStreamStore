namespace SqlStreamStore
{
    using System;
    using System.Data;
    using System.Runtime.ExceptionServices;
    using System.Threading;
    using System.Threading.Tasks;
    using MySql.Data.MySqlClient;
    using SqlStreamStore.Imports.Ensure.That;
    using SqlStreamStore.Infrastructure;
    using SqlStreamStore.Streams;
    using StreamStoreStore.Json;

    public partial class MySqlStreamStore
    {
        private static readonly string s_valueSeparator = "," + Environment.NewLine;
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

            MySqlAppendResult result;

            using(var connection = _createConnection())
            {
                await connection.OpenAsync(cancellationToken).NotOnCapturedContext();
                var streamIdInfo = new StreamIdInfo(streamId);

                using(var transaction = await connection.BeginTransactionAsync(cancellationToken))
                {
                    result = await AppendToStreamInternal(connection,
                        transaction,
                        streamIdInfo.SqlStreamId,
                        expectedVersion,
                        messages,
                        cancellationToken);
                }
            }

            if(result.MaxCount.HasValue)
            {
                await CheckStreamMaxCount(streamId, result.MaxCount.Value, cancellationToken);
            }

            return new AppendResult(result.CurrentVersion, result.CurrentPosition);
        }

        private Task<MySqlAppendResult> AppendToStreamInternal(
            MySqlConnection connection,
            MySqlTransaction transaction,
            MySqlStreamId streamId,
            int expectedVersion,
            NewStreamMessage[] messages,
            CancellationToken cancellationToken)
        {
            GuardAgainstDisposed();

            return RetryOnDeadLock(() =>
            {
                GuardAgainstDisposed();

                switch(expectedVersion)
                {
                    case ExpectedVersion.NoStream:
                        return AppendToStreamExpectedVersionNoStream(
                            connection,
                            transaction,
                            streamId,
                            messages,
                            cancellationToken);

                    case ExpectedVersion.Any:
                        return AppendToStreamExpectedVersionAny(
                            connection,
                            transaction,
                            streamId,
                            messages,
                            cancellationToken);
                    default:
                        return AppendToStreamExpectedVersion(
                            connection,
                            transaction,
                            streamId,
                            expectedVersion,
                            messages,
                            cancellationToken);
                }
            });
        }

        private async Task<MySqlAppendResult> AppendToStreamExpectedVersionNoStream(
            MySqlConnection connection,
            MySqlTransaction transaction,
            MySqlStreamId streamId,
            NewStreamMessage[] messages,
            CancellationToken cancellationToken)
        {
            using(var command = GetAppendCommand(
                connection,
                transaction,
                _scripts.AppendStreamExpectedVersionNoStream,
                _scripts.AppendStreamExpectedVersionNoStreamEmpty,
                messages))
            {
                command.Parameters.AddWithValue("streamId", streamId.Id);
                command.Parameters.AddWithValue("streamIdOriginal", streamId.IdOriginal);

                try
                {
                    using(var reader = await command
                        .ExecuteReaderAsync(cancellationToken)
                        .NotOnCapturedContext())
                    {
                        await reader.ReadAsync(cancellationToken).NotOnCapturedContext();
                        var currentVersion = reader.GetInt32(0);
                        var currentPosition = reader.GetInt64(1);
                        int? maxCount = null;

                        if(await reader.ReadAsync(cancellationToken).NotOnCapturedContext())
                        {
                            var jsonData = reader.GetString(2);
                            var metadataMessage = SimpleJson.DeserializeObject<MetadataMessage>(jsonData);
                            maxCount = metadataMessage.MaxCount;
                        }
                        return new MySqlAppendResult(maxCount, currentVersion, currentPosition);
                    }
                }
                catch(MySqlException ex)
                    when(ex.IsUniqueConstraintViolationOnIndex("IX_Streams_Id"))
                {
                    // Idempotency handling. Check if the Messages have already been written.
                    var page = await ReadStreamInternal(
                            streamId,
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
                            ErrorMessages.AppendFailedWrongExpectedVersion(streamId.IdOriginal,
                                ExpectedVersion.NoStream),
                            ex);
                    }

                    for(int i = 0; i < Math.Min(messages.Length, page.Messages.Length); i++)
                    {
                        if(messages[i].MessageId != page.Messages[i].MessageId)
                        {
                            throw new WrongExpectedVersionException(
                                ErrorMessages.AppendFailedWrongExpectedVersion(streamId.IdOriginal,
                                    ExpectedVersion.NoStream),
                                ex);
                        }
                    }

                    return new MySqlAppendResult(
                        null,
                        page.LastStreamVersion,
                        page.LastStreamPosition);

                }
                catch(MySqlException ex)
                    when(ex.IsUniqueConstraintViolation())
                {
                    throw new WrongExpectedVersionException(
                        ErrorMessages.AppendFailedWrongExpectedVersion(streamId.IdOriginal,
                            ExpectedVersion.NoStream),
                        ex);
                }
            }
        }

        private async Task<MySqlAppendResult> AppendToStreamExpectedVersionAny(
            MySqlConnection connection,
            MySqlTransaction transaction,
            MySqlStreamId streamId,
            NewStreamMessage[] messages,
            CancellationToken cancellationToken)
        {
            using(var command = GetAppendCommand(
                connection,
                transaction,
                _scripts.AppendStreamExpectedVersionAny,
                _scripts.AppendStreamExpectedVersionAnyEmpty,
                messages))
            {
                command.Parameters.AddWithValue("streamId", streamId.Id);
                command.Parameters.AddWithValue("streamIdOriginal", streamId.IdOriginal);

                try
                {
                    using(var reader = await command
                        .ExecuteReaderAsync(cancellationToken)
                        .NotOnCapturedContext())
                    {
                        await reader.ReadAsync(cancellationToken).NotOnCapturedContext();
                        var currentVersion = reader.GetInt32(0);
                        var currentPosition = reader.GetInt64(1);
                        int? maxCount = null;

                        if(await reader.ReadAsync(cancellationToken).NotOnCapturedContext())
                        {
                            var jsonData = reader.GetString(2);
                            var metadataMessage = SimpleJson.DeserializeObject<MetadataMessage>(jsonData);
                            maxCount = metadataMessage.MaxCount;
                        }
                        return new MySqlAppendResult(maxCount, currentVersion, currentPosition);
                    }
                }
                catch(MySqlException ex)
                    when(ex.IsUniqueConstraintViolationOnIndex("IX_Messages_StreamIdInternal_Id"))
                {
                    var streamVersion = await GetStreamVersionOfMessageId(
                        connection,
                        transaction,
                        streamId,
                        messages[0].MessageId,
                        cancellationToken);

                    // Idempotency handling. Check if the Messages have already been written.
                    var page = await ReadStreamInternal(
                            streamId,
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
                            ErrorMessages.AppendFailedWrongExpectedVersion(streamId.IdOriginal, ExpectedVersion.Any),
                            ex);
                    }

                    for(int i = 0; i < Math.Min(messages.Length, page.Messages.Length); i++)
                    {
                        if(messages[i].MessageId != page.Messages[i].MessageId)
                        {
                            throw new WrongExpectedVersionException(
                                ErrorMessages.AppendFailedWrongExpectedVersion(streamId.IdOriginal, ExpectedVersion.Any),
                                ex);
                        }
                    }
                    return new MySqlAppendResult(
                        null,
                        page.LastStreamVersion,
                        page.LastStreamPosition);
                }
                catch(MySqlException ex)
                    when(ex.IsUniqueConstraintViolation())
                {
                    throw new WrongExpectedVersionException(
                        ErrorMessages.AppendFailedWrongExpectedVersion(streamId.IdOriginal, ExpectedVersion.Any),
                        ex);

                }
            }
        }

        private async Task<MySqlAppendResult> AppendToStreamExpectedVersion(
            MySqlConnection connection,
            MySqlTransaction transaction,
            MySqlStreamId streamId,
            int expectedVersion,
            NewStreamMessage[] messages,
            CancellationToken cancellationToken)
        {

            var streamIdInternal = await GetStreamIdInternal(
                streamId,
                connection,
                transaction,
                cancellationToken);

            if(streamIdInternal == default(int?))
            {
                throw new WrongExpectedVersionException(
                    ErrorMessages.AppendFailedWrongExpectedVersion(
                        streamId.IdOriginal, expectedVersion));
            }

            var latestStreamVersion = await GetLatestStreamVersion(
                streamIdInternal.Value,
                connection,
                transaction,
                cancellationToken);

            if(latestStreamVersion != expectedVersion)
            {
                // Idempotency handling. Check if the Messages have already been written.

                var page = await ReadStreamInternal(
                    streamId,
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
                        ErrorMessages.AppendFailedWrongExpectedVersion(streamId.IdOriginal, expectedVersion));
                }

                // Iterate all messages an check to see if all message ids match
                for(int i = 0; i < Math.Min(messages.Length, page.Messages.Length); i++)
                {
                    if(messages[i].MessageId != page.Messages[i].MessageId)
                    {
                        throw new WrongExpectedVersionException(
                            ErrorMessages.AppendFailedWrongExpectedVersion(streamId.IdOriginal, expectedVersion));
                    }
                }

                return new MySqlAppendResult(
                    null,
                    page.LastStreamVersion,
                    page.LastStreamPosition);
            }

            try
            {
                using(var command = GetAppendCommand(
                    connection,
                    transaction,
                    _scripts.AppendStreamExpectedVersion,
                    _scripts.AppendStreamExpectedVersionEmpty,
                    messages))
                {
                    command.Parameters.AddWithValue("streamId", streamId.Id);
                    command.Parameters.AddWithValue("expectedVersion", expectedVersion);

                    using(var reader = await command
                        .ExecuteReaderAsync(cancellationToken)
                        .NotOnCapturedContext())
                    {
                        await reader.ReadAsync(cancellationToken).NotOnCapturedContext();
                        var currentVersion = reader.GetInt32(0);
                        var currentPosition = reader.GetInt64(1);
                        int? maxCount = null;

                        if(await reader.ReadAsync(cancellationToken).NotOnCapturedContext())
                        {
                            var jsonData = reader.GetString(2);
                            var metadataMessage = SimpleJson.DeserializeObject<MetadataMessage>(jsonData);
                            maxCount = metadataMessage.MaxCount;
                        }

                        return new MySqlAppendResult(maxCount, currentVersion, currentPosition);
                    }
                }
            }
            catch(MySqlException ex)
                when(ex.IsUniqueConstraintViolation())
            {
                throw new WrongExpectedVersionException(
                    ErrorMessages.AppendFailedWrongExpectedVersion(streamId.IdOriginal, expectedVersion),
                    ex);
            }
        }

        // Deadlocks appear to be a fact of life when there is high contention on a table regardless of
        // transaction isolation settings.
        private static async Task<T> RetryOnDeadLock<T>(Func<Task<T>> operation)
        {
            const int maxRetries = 2; //TODO too much? too little? configurable?
            Exception exception;

            int retryCount = 0;
            do
            {
                try
                {
                    return await operation();
                }
                catch(MySqlException ex) when(ex.Number == 1205 || ex.Number == 1213) // Deadlock error codes;
                {
                    exception = ex;
                    retryCount++;
                }
            } while(retryCount < maxRetries);

            ExceptionDispatchInfo.Capture(exception).Throw();
            return default(T); // never actually run
        }

        private async Task<int> GetStreamVersionOfMessageId(
            MySqlConnection connection,
            MySqlTransaction transaction,
            MySqlStreamId streamId,
            Guid messageId,
            CancellationToken cancellationToken)
        {
            using(var command = new MySqlCommand(_scripts.GetStreamVersionOfMessageId, connection, transaction))
            {
                command.Parameters.AddWithValue("streamId", streamId.Id);
                command.Parameters.AddWithValue("messageId", messageId);

                var result = await command.ExecuteScalarAsync(cancellationToken)
                    .NotOnCapturedContext();

                return (int) result;
            }
        }
        private (MySqlStreamMessageValues[] values, string commandText) Something(string script, NewStreamMessage[] messages)
        {
            int index = 0;
            var values = Array.ConvertAll(
                messages,
                message => new MySqlStreamMessageValues(index++, UtcNow, message));

            var sqlSnippet = string.Join(s_valueSeparator, Array.ConvertAll(values, value => value.SqlSnippet));

            return(values, string.Format(script, sqlSnippet));
        }

        private MySqlCommand GetAppendCommand(
            MySqlConnection connection,
            MySqlTransaction transaction,
            string script,
            string emptyMessagesScript,
            NewStreamMessage[] messages)
        {
            if(messages.Length == 0)
            {
                return new MySqlCommand(emptyMessagesScript, connection, transaction);
            }

            var (values, commandText) = Something(script, messages);
            var command = new MySqlCommand(commandText, connection, transaction);

            foreach(var value in values)
            {
                command.Parameters.AddRange(value.GetParameters());
            }

            return command;
        }


        private struct MySqlStreamMessageValues
        {
            public NewStreamMessage Message { get; }
            public DateTime Created { get; }
            public int Index { get; }

            public MySqlStreamMessageValues(int index, DateTime created, NewStreamMessage message)
            {
                Created = created;
                Message = message;
                Index = index;
            }

            public MySqlParameter[] GetParameters() => new[]
            {
                new MySqlParameter
                {
                    ParameterName = $"streamVersion_{Index}",
                    Value = Index
                },
                new MySqlParameter
                {
                    DbType = DbType.Binary,
                    ParameterName = $"id_{Index}",
                    Value = Message.MessageId.ToByteArray()
                },
                new MySqlParameter
                {
                    ParameterName = $"created_{Index}",
                    Value = Created.Ticks
                },
                new MySqlParameter
                {
                    ParameterName = $"type_{Index}",
                    Value = Message.Type
                },
                new MySqlParameter
                {
                    ParameterName = $"jsonData_{Index}",
                    Value = Message.JsonData
                },
                new MySqlParameter
                {
                    ParameterName = $"jsonMetadata_{Index}",
                    Value = Message.JsonMetadata
                }
            };

            public string SqlSnippet => $"(@streamIdInternal, ?streamVersion_{Index} + @latestStreamVersion + 1, ?id_{Index}, ?created_{Index}, ?type_{Index}, ?jsonData_{Index}, ?jsonMetadata_{Index})";

        }

        private struct MySqlAppendResult
        {
            public readonly int? MaxCount;
            public readonly int CurrentVersion;
            public readonly long CurrentPosition;

            public MySqlAppendResult(int? maxCount, int currentVersion, long currentPosition)
            {
                MaxCount = maxCount;
                CurrentVersion = currentVersion;
                CurrentPosition = currentPosition;
            }
        }

    }
}