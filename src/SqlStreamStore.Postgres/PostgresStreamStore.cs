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
            : base(settings.MetadataMaxAgeCacheExpire,
                settings.MetadataMaxAgeCacheMaxSize,
                settings.GetUtcNow,
                settings.LogName)
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
                    using(var command = new NpgsqlCommand($"CREATE SCHEMA IF NOT EXISTS {_settings.Schema}",
                        connection,
                        transaction))
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

        protected override Task<int> GetStreamMessageCount(
            string streamId,
            CancellationToken cancellationToken = new CancellationToken())
        {
            throw new NotImplementedException();
        }

        public Task<int> GetmessageCount(
            string streamId,
            DateTime TODO_WHAT_IS_THIS_FOR,
            CancellationToken cancellationToken = new CancellationToken())
        {
            throw new NotImplementedException();
        }

        protected override Task DeleteStreamInternal(
            string streamId,
            int expectedVersion,
            CancellationToken cancellationToken)
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

        public async Task DropAll(CancellationToken cancellationToken = default(CancellationToken))
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

        private Func<CancellationToken, Task<string>> GetJsonData(PostgresqlStreamId streamId, int version)
            => async cancellationToken =>
            {
                using(var connection = _createConnection())
                {
                    await connection.OpenAsync(cancellationToken);
                    using(var command = new NpgsqlCommand($"{_settings.Schema}.read_json_data", connection)
                    {
                        CommandType = CommandType.StoredProcedure,
                        Parameters =
                        {
                            new NpgsqlParameter
                            {
                                NpgsqlDbType = NpgsqlDbType.Varchar,
                                Size = 42,
                                NpgsqlValue = streamId.Id
                            },
                            new NpgsqlParameter
                            {
                                NpgsqlDbType = NpgsqlDbType.Integer,
                                Value = version
                            }
                        }
                    })
                    {
                        var jsonData = (string) await command
                            .ExecuteScalarAsync(cancellationToken)
                            .NotOnCapturedContext();
                        return jsonData;
                    }
                }
            };


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