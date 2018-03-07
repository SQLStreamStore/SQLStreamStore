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
    using SqlStreamStore.Subscriptions;

    public partial class PostgresStreamStore : StreamStoreBase
    {
        private readonly PostgresStreamStoreSettings _settings;
        private readonly Func<NpgsqlConnection> _createConnection;
        private readonly Scripts _scripts;
        
        public PostgresStreamStore(PostgresStreamStoreSettings settings)
            : base(settings.MetadataMaxAgeCacheExpire, settings.MetadataMaxAgeCacheMaxSize, settings.GetUtcNow, settings.LogName)
        {
            _settings = settings;
            _createConnection = () =>
            {
                var connection = new NpgsqlConnection(settings.ConnectionString);

                return connection;
            };
            _scripts = new Scripts(_settings.Schema);
        }

        public async Task CreateSchema(CancellationToken cancellationToken = default(CancellationToken))
        {
            using(var connection = _createConnection())
            {
                await connection.OpenAsync(cancellationToken).NotOnCapturedContext();
                
                //connection.MapComposite<PostgresNewStreamMessage>($"{_settings.Schema}.new_stream_message");

                using(var transaction = connection.BeginTransaction())
                {
                    using(var command = new NpgsqlCommand($"CREATE SCHEMA IF NOT EXISTS {_settings.Schema}", connection, transaction))
                    {
                        await command.ExecuteNonQueryAsync(cancellationToken).NotOnCapturedContext();
                    }
                    using(var command = new NpgsqlCommand(_scripts.CreateSchema, connection, transaction))
                    {
                        await command.ExecuteNonQueryAsync(cancellationToken).NotOnCapturedContext();
                    }

                    await transaction.CommitAsync(cancellationToken).NotOnCapturedContext();
                }
            }
        }

        protected override IStreamSubscription SubscribeToStreamInternal(
            string streamId,
            int? startVersion,
            StreamMessageReceived streamMessageReceived,
            SubscriptionDropped subscriptionDropped,
            HasCaughtUp hasCaughtUp,
            bool prefetchJsonData,
            string name)
        {
            throw new NotImplementedException();
        }

        protected override IAllStreamSubscription SubscribeToAllInternal(
            long? fromPosition,
            AllStreamMessageReceived streamMessageReceived,
            AllSubscriptionDropped subscriptionDropped,
            HasCaughtUp hasCaughtUp,
            bool prefetchJsonData,
            string name)
        {
            throw new NotImplementedException();
        }

        protected override Task<int> GetStreamMessageCount(string streamId, CancellationToken cancellationToken = new CancellationToken())
        {
            throw new NotImplementedException();
        }

        public Task<int> GetmessageCount(string streamId, DateTime TODO_WHAT_IS_THIS_FOR, CancellationToken cancellationToken = new CancellationToken())
        {
            throw new NotImplementedException();
        }

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

                //using(var transaction = connection.BeginTransaction(IsolationLevel.ReadCommitted))
                using(var command = new NpgsqlCommand($"{_settings.Schema}.append_to_stream", connection)
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
                {
                    var reader = await command.ExecuteReaderAsync(cancellationToken).NotOnCapturedContext();

                    await reader.ReadAsync(cancellationToken).NotOnCapturedContext();
                    
                    return new AppendResult(reader.GetInt32(0), reader.GetInt64(1));
                }
            }
        }

        protected override Task DeleteStreamInternal(string streamId, int expectedVersion, CancellationToken cancellationToken)
        {
            throw new NotImplementedException();
        }

        protected override Task DeleteEventInternal(string streamId, Guid eventId, CancellationToken cancellationToken)
        {
            throw new NotImplementedException();
        }

        protected override Task<SetStreamMetadataResult> SetStreamMetadataInternal(
            string streamId,
            int expectedStreamMetadataVersion,
            int? maxAge,
            int? maxCount,
            string metadataJson,
            CancellationToken cancellationToken)
        {
            throw new NotImplementedException();
        }

        public async Task DropAll(CancellationToken cancellationToken = default (CancellationToken))
        {
            using(var connection = _createConnection())
            {
                await connection.OpenAsync(cancellationToken).NotOnCapturedContext();

                using(var command = new NpgsqlCommand(_scripts.DropAll, connection))
                {
                    await command
                        .ExecuteNonQueryAsync(cancellationToken)
                        .NotOnCapturedContext();
                }
            }
        }

        private class ReadStreamParameters
        {
            private ReadStreamParameters()
            {
                
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

        private class PostgresStreamMessageWithJson
        {
            public long Position { get; set; }
            public int StreamVersion { get; set; }
            public Guid MessageId { get; set; }
            public string StreamId { get; set; }
            public DateTime CreatedUtc { get; set; }
            public string JsonData { get; set; }
            public string JsonMetadata { get; set; }
            public string Type { get; set; }

            public static explicit operator StreamMessage(PostgresStreamMessageWithJson message)
                => new StreamMessage(
                    message.StreamId,
                    message.MessageId,
                    message.StreamVersion,
                    message.Position,
                    message.CreatedUtc,
                    message.Type,
                    message.JsonMetadata,
                    message.JsonData);
        }
    }
}
