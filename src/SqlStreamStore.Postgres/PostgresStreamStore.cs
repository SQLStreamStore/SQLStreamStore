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
    using SqlStreamStore.Subscriptions;

    public partial class PostgresStreamStore : StreamStoreBase
    {
        private readonly PostgresStreamStoreSettings _settings;
        private readonly Func<NpgsqlConnection> _createConnection;
        private readonly Schema _schema;
        private readonly Lazy<IStreamStoreNotifier> _streamStoreNotifier;

        public PostgresStreamStore(PostgresStreamStoreSettings settings)
            : base(
                settings.MetadataMaxAgeCacheExpire,
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
            _streamStoreNotifier = new Lazy<IStreamStoreNotifier>(() =>
            {
                if(settings.CreateStreamStoreNotifier == null)
                {
                    throw new InvalidOperationException(
                        "Cannot create notifier because supplied createStreamStoreNotifier was null");
                }
                return settings.CreateStreamStoreNotifier.Invoke(this);
            });
            _schema = new Schema(_settings.Schema);
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

                    using(var command = new NpgsqlCommand(_schema.Definition, connection, transaction))
                    {
                        await command.ExecuteNonQueryAsync(cancellationToken).NotOnCapturedContext();
                    }

                    await transaction.CommitAsync(cancellationToken).NotOnCapturedContext();
                }
            }
        }

        protected override Task<int> GetStreamMessageCount(
            string streamId,
            CancellationToken cancellationToken = new CancellationToken())
        {
            throw new NotImplementedException();
        }

        public async Task<int> GetMessageCount(
            string streamId,
            DateTime createdBefore,
            CancellationToken cancellationToken = new CancellationToken())
        {
            GuardAgainstDisposed();

            var streamIdInfo = new StreamIdInfo(streamId);

            using(var connection = _createConnection())
            {
                await connection.OpenAsync(cancellationToken).NotOnCapturedContext();

                using(var command = new NpgsqlCommand(_schema.ReadStreamMessageBeforeCreatedCount, connection)
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
                            NpgsqlDbType = NpgsqlDbType.Timestamp,
                            NpgsqlValue = createdBefore
                        }
                    }
                })
                {

                    var result = await command.ExecuteScalarAsync(cancellationToken).NotOnCapturedContext();

                    return (int) result;
                }
            }
        }

        public async Task DropAll(CancellationToken cancellationToken = default(CancellationToken))
        {
            using(var connection = _createConnection())
            {
                await connection.OpenAsync(cancellationToken).NotOnCapturedContext();

                using(var command = new NpgsqlCommand(_schema.DropAll, connection))
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
                    using(var command = new NpgsqlCommand(_schema.ReadJsonData, connection)
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
                        var jsonData = await command
                            .ExecuteScalarAsync(cancellationToken)
                            .NotOnCapturedContext();
                        return jsonData == DBNull.Value ? null : (string)jsonData;
                    }
                }
            };
    }
}