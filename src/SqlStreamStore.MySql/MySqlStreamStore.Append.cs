namespace SqlStreamStore
{
    using System;
    using System.Threading;
    using System.Threading.Tasks;
    using MySql.Data.MySqlClient;
    using SqlStreamStore.Infrastructure;
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
                .NotOnCapturedContext())
            {
                foreach(var message in messages)
                {
                    (nextExpectedVersion, appendResult) = await AppendMessageToStream(
                        streamId,
                        nextExpectedVersion,
                        message,
                        transaction,
                        cancellationToken);
                }

                if(appendResult.CurrentVersion - Math.Max(expectedVersion, 0) + 1 < +messages.Length)
                {
                    await transaction.RollbackAsync(cancellationToken).NotOnCapturedContext();
                    throw new WrongExpectedVersionException(
                        ErrorMessages.AppendFailedWrongExpectedVersion(
                            streamId.MySqlStreamId.IdOriginal,
                            expectedVersion),
                        streamId.MySqlStreamId.IdOriginal,
                        expectedVersion);
                }

                await transaction.CommitAsync(cancellationToken).NotOnCapturedContext();
            }

            await TryScavenge(streamId, cancellationToken).NotOnCapturedContext();

            return appendResult;
        }

        private Task<(int nextExpectedVersion, AppendResult)> AppendMessageToStream(
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

        private async Task<(int nextExpectedVersion, AppendResult)> AppendToStreamExpectedVersionAny(
            StreamIdInfo streamId,
            NewStreamMessage message,
            MySqlTransaction transaction,
            CancellationToken cancellationToken)
        {
            var currentVersion = Parameters.CurrentVersion();
            var currentPosition = Parameters.CurrentPosition();

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
                currentPosition))
            {
                var nextExpectedVersion = Convert.ToInt32(
                    await command
                        .ExecuteScalarAsync(cancellationToken)
                        .NotOnCapturedContext());
                return (
                    nextExpectedVersion,
                    new AppendResult((int) currentVersion.Value, (long) currentPosition.Value));
            }
        }

        private async Task<(int nextExpectedVersion, AppendResult)> AppendToStreamExpectedVersionNoStream(
            StreamIdInfo streamId,
            NewStreamMessage message,
            MySqlTransaction transaction,
            CancellationToken cancellationToken)
        {
            var currentVersion = Parameters.CurrentVersion();
            var currentPosition = Parameters.CurrentPosition();

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
                currentPosition))
            {
                var nextExpectedVersion = Convert.ToInt32(
                    await command
                        .ExecuteScalarAsync(cancellationToken)
                        .NotOnCapturedContext());
                return (
                    nextExpectedVersion,
                    new AppendResult((int) currentVersion.Value, (long) currentPosition.Value));
            }
        }

        private async Task<(int nextExpectedVersion, AppendResult)> AppendToStreamExpectedVersionEmptyStream(
            StreamIdInfo streamId,
            NewStreamMessage message,
            MySqlTransaction transaction,
            CancellationToken cancellationToken)
        {
            var currentVersion = Parameters.CurrentVersion();
            var currentPosition = Parameters.CurrentPosition();

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
                currentPosition))
            {
                var nextExpectedVersion = Convert.ToInt32(
                    await command
                        .ExecuteScalarAsync(cancellationToken)
                        .NotOnCapturedContext());
                return (
                    nextExpectedVersion,
                    new AppendResult((int) currentVersion.Value, (long) currentPosition.Value));
            }
        }

        private async Task<(int nextExpectedVersion, AppendResult)> AppendToStreamExpectedVersion(
            StreamIdInfo streamId,
            int expectedVersion,
            NewStreamMessage message,
            MySqlTransaction transaction,
            CancellationToken cancellationToken)
        {
            var currentVersion = Parameters.CurrentVersion();
            var currentPosition = Parameters.CurrentPosition();

            using(var command = BuildStoredProcedureCall(
                _schema.AppendToStreamExpectedVersion,
                transaction,
                Parameters.StreamId(streamId.MySqlStreamId),
                Parameters.StreamIdOriginal(streamId.MySqlStreamId),
                Parameters.ExpectedVersion(expectedVersion),
                Parameters.CreatedUtc(_settings.GetUtcNow?.Invoke()),
                Parameters.MessageId(message.MessageId),
                Parameters.Type(message.Type),
                Parameters.JsonData(message.JsonData),
                Parameters.JsonMetadata(message.JsonMetadata),
                currentVersion,
                currentPosition))
            {
                var nextExpectedVersion = Convert.ToInt32(
                    await command
                        .ExecuteScalarAsync(cancellationToken)
                        .NotOnCapturedContext());
                return (
                    nextExpectedVersion,
                    new AppendResult((int) currentVersion.Value, (long) currentPosition.Value));
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
                .NotOnCapturedContext())
            using(var command = BuildStoredProcedureCall(_schema.CreateEmptyStream,
                transaction,
                Parameters.StreamId(streamId.MySqlStreamId),
                Parameters.StreamIdOriginal(streamId.MySqlStreamId),
                Parameters.MetadataStreamId(streamId.MetadataMySqlStreamId),
                Parameters.ExpectedVersion(expectedVersion)))
            using(var reader = await command.ExecuteReaderAsync(cancellationToken).NotOnCapturedContext())
            {
                await reader.ReadAsync(cancellationToken).NotOnCapturedContext();

                appendResult = new AppendResult(reader.GetInt32(0), reader.GetInt64(1));

                reader.Close();

                await transaction.CommitAsync(cancellationToken).NotOnCapturedContext();
            }

            return appendResult;
        }
    }
}