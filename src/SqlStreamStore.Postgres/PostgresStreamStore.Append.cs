namespace SqlStreamStore
{
    using System;
    using System.Data;
    using System.Threading;
    using System.Threading.Tasks;
    using Npgsql;
    using NpgsqlTypes;
    using SqlStreamStore.Infrastructure;
    using SqlStreamStore.PgSqlScripts;
    using SqlStreamStore.Streams;

    partial class PostgresStreamStore
    {
        protected override async Task<AppendResult> AppendToStreamInternal(
            string streamId,
            int expectedVersion,
            NewStreamMessage[] messages,
            CancellationToken cancellationToken)
        {
            var streamIdInfo = new StreamIdInfo(streamId);

            using(var connection = _createConnection())
            {
                await connection.OpenAsync(cancellationToken).NotOnCapturedContext();
                using(var transaction = connection.BeginTransaction(IsolationLevel.ReadCommitted))
                {
                    var result = await AppendToStreamInternal(
                        streamIdInfo.PostgresqlStreamId,
                        expectedVersion,
                        messages,
                        transaction,
                        cancellationToken);

                    await transaction.CommitAsync(cancellationToken).NotOnCapturedContext();

                    return result;
                }
            }
        }

        private async Task<AppendResult> AppendToStreamInternal(
            PostgresqlStreamId streamId,
            int expectedVersion,
            NewStreamMessage[] messages,
            NpgsqlTransaction transaction,
            CancellationToken cancellationToken)
        {
            transaction.Connection.MapComposite<PostgresNewStreamMessage>(_schema.NewStreamMessage);

            using(var command = new NpgsqlCommand(_schema.AppendToStream, transaction.Connection, transaction)
            {
                CommandType = CommandType.StoredProcedure,
                Parameters =
                {
                    new NpgsqlParameter
                    {
                        NpgsqlDbType = NpgsqlDbType.Char,
                        Size = 42,
                        NpgsqlValue = streamId.Id
                    },
                    new NpgsqlParameter
                    {
                        NpgsqlDbType = NpgsqlDbType.Varchar,
                        Size = 1000,
                        NpgsqlValue = streamId.IdOriginal
                    },
                    new NpgsqlParameter
                    {
                        NpgsqlDbType = NpgsqlDbType.Integer,
                        NpgsqlValue = expectedVersion
                    },
                    new NpgsqlParameter
                    {
                        NpgsqlValue = _settings.GetUtcNow(),
                        NpgsqlDbType = NpgsqlDbType.Timestamp
                    },
                    new NpgsqlParameter
                    {
                        NpgsqlValue = Array.ConvertAll(messages, PostgresNewStreamMessage.FromNewStreamMessage)
                    }
                }
            })
            {
                try
                {
                    using(var reader = await command.ExecuteReaderAsync(cancellationToken).NotOnCapturedContext())
                    {
                        await reader.ReadAsync(cancellationToken).NotOnCapturedContext();

                        return new AppendResult(reader.GetInt32(0), reader.GetInt64(1));
                    }
                }
                catch(NpgsqlException ex) when(expectedVersion == ExpectedVersion.Any
                                               && ex.IsUniqueConstraintViolation(
                                                   Schema.MessagesByStreamIdInternalAndMessageId))
                {
                    await transaction.RollbackAsync(cancellationToken).NotOnCapturedContext();
                    
                    var page = await ReadStreamInternal(
                        streamId,
                        expectedVersion,
                        messages.Length,
                        ReadDirection.Forward,
                        false,
                        null,
                        transaction,
                        cancellationToken).NotOnCapturedContext();

                    ThrowIfWrongExpectedVersion(streamId, ExpectedVersion.Any, messages, page, ex);

                    return new AppendResult(page.LastStreamVersion, page.LastStreamPosition);
                }
                catch(NpgsqlException ex) when(expectedVersion == ExpectedVersion.Any
                                               && ex.IsUniqueConstraintViolation())
                {
                    throw new WrongExpectedVersionException(
                        ErrorMessages.AppendFailedWrongExpectedVersion(streamId.IdOriginal, ExpectedVersion.Any),
                        ex);
                }
                catch(NpgsqlException ex) when(expectedVersion == ExpectedVersion.NoStream
                                               && ex.IsUniqueConstraintViolation(Schema.StreamsById))
                {
                    await transaction.RollbackAsync(cancellationToken).NotOnCapturedContext();

                    var page = await ReadStreamInternal(
                            streamId,
                            StreamVersion.Start,
                            messages.Length,
                            ReadDirection.Forward,
                            false,
                            null,
                            transaction,
                            cancellationToken)
                        .NotOnCapturedContext();

                    ThrowIfWrongExpectedVersion(streamId, ExpectedVersion.NoStream, messages, page, ex);

                    return new AppendResult(page.LastStreamVersion, page.LastStreamPosition);
                }
                catch(NpgsqlException ex) when(expectedVersion == ExpectedVersion.NoStream
                                               && ex.IsUniqueConstraintViolation())
                {
                    throw new WrongExpectedVersionException(
                        ErrorMessages.AppendFailedWrongExpectedVersion(streamId.IdOriginal, ExpectedVersion.Any),
                        ex);
                }
                catch(NpgsqlException ex) when(ex.Message.Contains("WrongExpectedVersion"))
                {
                    var page = await ReadStreamInternal(
                        streamId,
                        expectedVersion + 1,
                        // when reading for already written Messages, it's from the one after the expected
                        messages.Length,
                        ReadDirection.Forward,
                        false,
                        null,
                        transaction,
                        cancellationToken);
                    
                    ThrowIfWrongExpectedVersion(streamId, expectedVersion, messages, page, ex);
                    
                    return new AppendResult(page.LastStreamVersion, page.LastStreamPosition);
                }
                catch(NpgsqlException ex) when(ex.IsUniqueConstraintViolation())
                {
                    throw new WrongExpectedVersionException(
                        ErrorMessages.AppendFailedWrongExpectedVersion(streamId.IdOriginal, expectedVersion),
                        ex);
                }
            }
        }

        private static void ThrowIfWrongExpectedVersion(
            PostgresqlStreamId streamId,
            int expectedVersion,
            NewStreamMessage[] messages,
            ReadStreamPage page,
            NpgsqlException inner)
        {
            if(messages.Length > page.Messages.Length)
            {
                throw new WrongExpectedVersionException(
                    ErrorMessages.AppendFailedWrongExpectedVersion(streamId.IdOriginal, expectedVersion),
                    inner);
            }

            for(int i = 0; i < Math.Min(messages.Length, page.Messages.Length); i++)
            {
                if(messages[i].MessageId != page.Messages[i].MessageId)
                {
                    throw new WrongExpectedVersionException(
                        ErrorMessages.AppendFailedWrongExpectedVersion(streamId.IdOriginal, expectedVersion),
                        inner);
                }
            }
        }

        private async Task<int> GetStreamVersionOfMessageId(
            PostgresqlStreamId streamId,
            Guid messageId,
            NpgsqlTransaction transaction,
            CancellationToken cancellationToken)
        {
            using(var command = new NpgsqlCommand(
                _schema.ReadStreamVersionOfMessageId,
                transaction.Connection,
                transaction)
            {
                CommandType = CommandType.StoredProcedure,
                Parameters =
                {
                    new NpgsqlParameter
                    {
                        NpgsqlDbType = NpgsqlDbType.Char,
                        Size = 42,
                        NpgsqlValue = streamId.Id
                    },
                    new NpgsqlParameter
                    {
                        NpgsqlDbType = NpgsqlDbType.Uuid,
                        Value = messageId
                    }
                }
            })
            {
                var result = await command.ExecuteScalarAsync(cancellationToken).NotOnCapturedContext();

                return (int) result;
            }
        }

        private class PostgresNewStreamMessage
        {
            public Guid MessageId { get; set; }
            public string JsonData { get; set; }
            public string JsonMetadata { get; set; }
            public string Type { get; set; }

            public static PostgresNewStreamMessage FromNewStreamMessage(NewStreamMessage message)
                => new PostgresNewStreamMessage
                {
                    MessageId = message.MessageId,
                    Type = message.Type,
                    JsonData = message.JsonData,
                    JsonMetadata = message.JsonMetadata
                };
        }
    }
}