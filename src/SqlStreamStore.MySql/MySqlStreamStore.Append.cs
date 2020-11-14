namespace SqlStreamStore
{
    using System;
    using System.Collections.Generic;
    using System.Threading;
    using System.Threading.Tasks;
    using MySqlConnector;
    using SqlStreamStore.MySqlScripts;
    using SqlStreamStore.Streams;

    partial class MySqlStreamStore
    {
        protected override async Task<AppendResult> AppendToStreamInternal(
            string streamId,
            int expectedVersion,
            NewStreamMessage[] messages,
            CancellationToken cancellationToken)
        {
            var streamIdInfo = new StreamIdInfo(streamId);

            if(_settings.AppendDeadlockRetryAttempts == 0)
            {
                try
                {
                    return messages.Length == 0
                        ? await CreateEmptyStream(streamIdInfo, expectedVersion, cancellationToken)
                        : await AppendMessagesToStream(streamIdInfo, expectedVersion, messages, cancellationToken);
                }
                catch(MySqlException ex) when(ex.IsWrongExpectedVersion())
                {
                    throw new WrongExpectedVersionException(
                        ErrorMessages.AppendFailedWrongExpectedVersion(
                            streamIdInfo.MySqlStreamId.IdOriginal,
                            expectedVersion),
                        streamIdInfo.MySqlStreamId.IdOriginal,
                        expectedVersion,
                        ex);
                }
            }

            var retryableExceptions = new List<Exception>();
            while(retryableExceptions.Count <= _settings.AppendDeadlockRetryAttempts)
            {
                try
                {
                    return messages.Length == 0
                        ? await CreateEmptyStream(streamIdInfo, expectedVersion, cancellationToken)
                        : await AppendMessagesToStream(streamIdInfo, expectedVersion, messages, cancellationToken);
                }
                catch(MySqlException ex) when(ex.IsDeadlock())
                {
                    retryableExceptions.Add(ex);
                }
                catch(MySqlException ex) when(ex.IsWrongExpectedVersion())
                {
                    throw new WrongExpectedVersionException(
                        ErrorMessages.AppendFailedWrongExpectedVersion(
                            streamIdInfo.MySqlStreamId.IdOriginal,
                            expectedVersion),
                        streamIdInfo.MySqlStreamId.IdOriginal,
                        expectedVersion,
                        ex);
                }
            }
            throw new WrongExpectedVersionException(
                MySqlErrorMessages.AppendFailedDeadlock(
                    streamIdInfo.MySqlStreamId.IdOriginal,
                    expectedVersion,
                    _settings.AppendDeadlockRetryAttempts),
                streamIdInfo.MySqlStreamId.IdOriginal,
                expectedVersion,
                new AggregateException(retryableExceptions));


        }

        private async Task<AppendResult> AppendMessagesToStream(
            StreamIdInfo streamId,
            int expectedVersion,
            NewStreamMessage[] messages,
            CancellationToken cancellationToken)
        {
            var appendResult = new AppendResult(StreamVersion.End, Position.End);
            var nextExpectedVersion = expectedVersion;

            using(var connection = await OpenConnection(cancellationToken))
            using(var transaction = await connection
                .BeginTransactionAsync(cancellationToken)
                .ConfigureAwait(false))
            {
                var throwIfAdditionalMessages = false;

                for(var i = 0; i < messages.Length; i++)
                {
                    bool messageExists;
                    (nextExpectedVersion, appendResult, messageExists) = await AppendMessageToStream(
                        streamId,
                        nextExpectedVersion,
                        messages[i],
                        transaction,
                        cancellationToken);

                    if(i == 0)
                    {
                        throwIfAdditionalMessages = messageExists;
                    }
                    else
                    {
                        if(throwIfAdditionalMessages && !messageExists)
                        {
                            throw new WrongExpectedVersionException(
                                ErrorMessages.AppendFailedWrongExpectedVersion(
                                    streamId.MySqlStreamId.IdOriginal,
                                    expectedVersion),
                                streamId.MySqlStreamId.IdOriginal,
                                expectedVersion);
                        }
                    }
                }

                await transaction.CommitAsync(cancellationToken).ConfigureAwait(false);
            }

            await TryScavenge(streamId, cancellationToken).ConfigureAwait(false);

            return appendResult;
        }

        private Task<(int nextExpectedVersion, AppendResult appendResult, bool messageExists)> AppendMessageToStream(
            StreamIdInfo streamId,
            int expectedVersion,
            NewStreamMessage message,
            MySqlTransaction transaction,
            CancellationToken cancellationToken)
        {
            switch(expectedVersion)
            {
                case ExpectedVersion.Any:
                    return AppendToStreamExpectedVersionAny(streamId, message, transaction, cancellationToken);
                case ExpectedVersion.NoStream:
                    return AppendToStreamExpectedVersionNoStream(streamId, message, transaction, cancellationToken);
                case ExpectedVersion.EmptyStream:
                    return AppendToStreamExpectedVersionEmptyStream(streamId, message, transaction, cancellationToken);
                default:
                    return AppendToStreamExpectedVersion(
                        streamId,
                        expectedVersion,
                        message,
                        transaction,
                        cancellationToken);
            }
        }

        private async Task<(int nextExpectedVersion, AppendResult appendResult, bool messageExists)>
            AppendToStreamExpectedVersionAny(
                StreamIdInfo streamId,
                NewStreamMessage message,
                MySqlTransaction transaction,
                CancellationToken cancellationToken)
        {
            var currentVersion = Parameters.CurrentVersion();
            var currentPosition = Parameters.CurrentPosition();
            var messageExists = Parameters.MessageExists();

            using(var command = BuildStoredProcedureCall(
                _schema.AppendToStreamExpectedVersionAny,
                transaction,
                Parameters.StreamId(streamId.MySqlStreamId),
                Parameters.StreamIdOriginal(streamId.MySqlStreamId),
                Parameters.MetadataStreamId(streamId.MetadataMySqlStreamId),
                Parameters.CreatedUtc(_settings.GetUtcNow?.Invoke()),
                Parameters.MessageId(message.MessageId),
                Parameters.Type(message.Type),
                Parameters.JsonData(message.JsonData),
                Parameters.JsonMetadata(message.JsonMetadata),
                currentVersion,
                currentPosition,
                messageExists))
            {
                var nextExpectedVersion = Convert.ToInt32(
                    await command
                        .ExecuteScalarAsync(cancellationToken)
                        .ConfigureAwait(false));
                return (
                    nextExpectedVersion,
                    new AppendResult(
                        (int) currentVersion.Value,
                        (long) currentPosition.Value),
                    (bool) messageExists.Value);
            }
        }

        private async Task<(int nextExpectedVersion, AppendResult appendResult, bool messageExists)>
            AppendToStreamExpectedVersionNoStream(
                StreamIdInfo streamId,
                NewStreamMessage message,
                MySqlTransaction transaction,
                CancellationToken cancellationToken)
        {
            var currentVersion = Parameters.CurrentVersion();
            var currentPosition = Parameters.CurrentPosition();
            var messageExists = Parameters.MessageExists();

            using(var command = BuildStoredProcedureCall(
                _schema.AppendToStreamExpectedVersionNoStream,
                transaction,
                Parameters.StreamId(streamId.MySqlStreamId),
                Parameters.StreamIdOriginal(streamId.MySqlStreamId),
                Parameters.MetadataStreamId(streamId.MetadataMySqlStreamId),
                Parameters.CreatedUtc(_settings.GetUtcNow?.Invoke()),
                Parameters.MessageId(message.MessageId),
                Parameters.Type(message.Type),
                Parameters.JsonData(message.JsonData),
                Parameters.JsonMetadata(message.JsonMetadata),
                currentVersion,
                currentPosition,
                messageExists))
            {
                var nextExpectedVersion = Convert.ToInt32(
                    await command
                        .ExecuteScalarAsync(cancellationToken)
                        .ConfigureAwait(false));
                return (
                    nextExpectedVersion,
                    new AppendResult(
                        (int) currentVersion.Value,
                        (long) currentPosition.Value),
                    (bool) messageExists.Value);
            }
        }

        private async Task<(int nextExpectedVersion, AppendResult appendResult, bool messageExists)>
            AppendToStreamExpectedVersionEmptyStream(
                StreamIdInfo streamId,
                NewStreamMessage message,
                MySqlTransaction transaction,
                CancellationToken cancellationToken)
        {
            var currentVersion = Parameters.CurrentVersion();
            var currentPosition = Parameters.CurrentPosition();
            var messageExists = Parameters.MessageExists();

            using(var command = BuildStoredProcedureCall(
                _schema.AppendToStreamExpectedVersionEmptyStream,
                transaction,
                Parameters.StreamId(streamId.MySqlStreamId),
                Parameters.StreamIdOriginal(streamId.MySqlStreamId),
                Parameters.MetadataStreamId(streamId.MetadataMySqlStreamId),
                Parameters.CreatedUtc(_settings.GetUtcNow?.Invoke()),
                Parameters.MessageId(message.MessageId),
                Parameters.Type(message.Type),
                Parameters.JsonData(message.JsonData),
                Parameters.JsonMetadata(message.JsonMetadata),
                currentVersion,
                currentPosition,
                messageExists))
            {
                var nextExpectedVersion = Convert.ToInt32(
                    await command
                        .ExecuteScalarAsync(cancellationToken)
                        .ConfigureAwait(false));
                return (
                    nextExpectedVersion,
                    new AppendResult(
                        (int) currentVersion.Value,
                        (long) currentPosition.Value),
                    (bool) messageExists.Value);
            }
        }

        private async Task<(int nextExpectedVersion, AppendResult appendResult, bool messageExists)>
            AppendToStreamExpectedVersion(
                StreamIdInfo streamId,
                int expectedVersion,
                NewStreamMessage message,
                MySqlTransaction transaction,
                CancellationToken cancellationToken)
        {
            var currentVersion = Parameters.CurrentVersion();
            var currentPosition = Parameters.CurrentPosition();
            var messageExists = Parameters.MessageExists();

            using(var command = BuildStoredProcedureCall(
                _schema.AppendToStreamExpectedVersion,
                transaction,
                Parameters.StreamId(streamId.MySqlStreamId),
                Parameters.ExpectedVersion(expectedVersion),
                Parameters.CreatedUtc(_settings.GetUtcNow?.Invoke()),
                Parameters.MessageId(message.MessageId),
                Parameters.Type(message.Type),
                Parameters.JsonData(message.JsonData),
                Parameters.JsonMetadata(message.JsonMetadata),
                messageExists,
                currentVersion,
                currentPosition))
            {
                var nextExpectedVersion = Convert.ToInt32(
                    await command
                        .ExecuteScalarAsync(cancellationToken)
                        .ConfigureAwait(false));
                return (
                    nextExpectedVersion,
                    new AppendResult(
                        (int) currentVersion.Value,
                        (long) currentPosition.Value),
                    (bool) messageExists.Value);
            }
        }

        private async Task<AppendResult> CreateEmptyStream(
            StreamIdInfo streamId,
            int expectedVersion,
            CancellationToken cancellationToken)
        {
            var appendResult = new AppendResult(StreamVersion.End, Position.End);

            using(var connection = await OpenConnection(cancellationToken))
            using(var transaction = await connection
                .BeginTransactionAsync(cancellationToken)
                .ConfigureAwait(false))
            using(var command = BuildStoredProcedureCall(
                _schema.CreateEmptyStream,
                transaction,
                Parameters.StreamId(streamId.MySqlStreamId),
                Parameters.StreamIdOriginal(streamId.MySqlStreamId),
                Parameters.MetadataStreamId(streamId.MetadataMySqlStreamId),
                Parameters.ExpectedVersion(expectedVersion)))
            using(var reader = await command.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false))
            {
                await reader.ReadAsync(cancellationToken).ConfigureAwait(false);

                appendResult = new AppendResult(reader.GetInt32(0), reader.GetInt64(1));

                reader.Close();

                await transaction.CommitAsync(cancellationToken).ConfigureAwait(false);
            }

            return appendResult;
        }
    }
}
