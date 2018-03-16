namespace SqlStreamStore
{
    using System;
    using System.Data;
    using System.Threading;
    using System.Threading.Tasks;
    using Npgsql;
    using NpgsqlTypes;
    using SqlStreamStore.Infrastructure;
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

                connection.MapComposite<PostgresNewStreamMessage>($"{_settings.Schema}.new_stream_message");

                using(var transaction = connection.BeginTransaction(IsolationLevel.ReadCommitted))
                using(var command = new NpgsqlCommand($"{_settings.Schema}.append_to_stream", connection, transaction)
                {
                    CommandType = CommandType.StoredProcedure,
                    Parameters =
                    {
                        new NpgsqlParameter
                        {
                            NpgsqlDbType = NpgsqlDbType.Char,
                            Size = 42,
                            NpgsqlValue = streamIdInfo.PostgresqlStreamId.Id
                        },
                        new NpgsqlParameter
                        {
                            NpgsqlDbType = NpgsqlDbType.Varchar,
                            Size = 1000,
                            NpgsqlValue = streamIdInfo.PostgresqlStreamId.IdOriginal
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
                using(var reader = await command.ExecuteReaderAsync(cancellationToken).NotOnCapturedContext())
                {
                    await reader.ReadAsync(cancellationToken).NotOnCapturedContext();

                    var result = new AppendResult(reader.GetInt32(0), reader.GetInt64(1));

                    await transaction.CommitAsync(cancellationToken);

                    return result;
                }
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