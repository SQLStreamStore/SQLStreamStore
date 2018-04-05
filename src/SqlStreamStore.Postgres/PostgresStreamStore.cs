namespace SqlStreamStore
{
    using System;
    using System.Collections.Generic;
    using System.Data;
    using System.Threading;
    using System.Threading.Tasks;
    using Npgsql;
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
            using(var transaction = await BeginTransaction(connection, cancellationToken))
            using(var command = BuildCommand(
                _schema.ReadStreamMessageBeforeCreatedCount,
                transaction,
                Parameters.StreamId(streamIdInfo.PostgresqlStreamId),
                Parameters.CreatedUtc(createdBefore)))
            {
                var result = await command.ExecuteScalarAsync(cancellationToken).NotOnCapturedContext();

                return (int) result;
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
                using(var transaction = await BeginTransaction(connection, cancellationToken))
                using(var command = BuildCommand(
                    _schema.ReadJsonData,
                    transaction,
                    Parameters.StreamId(streamId),
                    Parameters.Version(version)))
                {
                    var jsonData = await command.ExecuteScalarAsync(cancellationToken).NotOnCapturedContext();

                    return jsonData == DBNull.Value ? null : (string) jsonData;
                }
            };

        private Task<NpgsqlTransaction> BeginTransaction(
            NpgsqlConnection connection,
            CancellationToken cancellationToken)
            => BeginTransaction(connection, IsolationLevel.ReadCommitted, cancellationToken);

        private async Task<NpgsqlTransaction> BeginTransaction(
            NpgsqlConnection connection,
            IsolationLevel isolationLevel,
            CancellationToken cancellationToken)
        {
            await connection.OpenAsync(cancellationToken).NotOnCapturedContext();

            return connection.BeginTransaction(isolationLevel);
        }

        private NpgsqlCommand BuildCommand(
            string procedure,
            NpgsqlTransaction transaction,
            params NpgsqlParameter[] parameters)
        {
            var command = new NpgsqlCommand(procedure, transaction.Connection, transaction)
            {
                CommandType = CommandType.StoredProcedure
            };

            foreach(var parameter in parameters)
            {
                command.Parameters.Add(parameter);
            }

            return command;
        }

        private async Task<int> TryScavenge(
            StreamIdInfo streamIdInfo,
            int? maxCount,
            CancellationToken cancellationToken)
        {
            if(streamIdInfo.PostgresqlStreamId == PostgresqlStreamId.Deleted)
            {
                return -1;
            }

            using(var connection = _createConnection())
            using(var transaction = await BeginTransaction(connection, cancellationToken))
            {
                var deletedMessageIds = new List<Guid>();
                using(var command = BuildCommand(
                    _schema.Scavenge,
                    transaction,
                    Parameters.StreamId(streamIdInfo.PostgresqlStreamId),
                    Parameters.OptionalMaxAge(maxCount)))
                using(var reader = await command.ExecuteReaderAsync(cancellationToken).NotOnCapturedContext())
                {
                    while(await reader.ReadAsync(cancellationToken).NotOnCapturedContext())
                    {
                        deletedMessageIds.Add(reader.GetGuid(0));
                    }
                }

                if(deletedMessageIds.Count > 0)
                {
                    await DeleteEventsInternal(
                        streamIdInfo,
                        deletedMessageIds.ToArray(),
                        transaction,
                        cancellationToken).NotOnCapturedContext();
                }

                await transaction.CommitAsync(cancellationToken).NotOnCapturedContext();

                return deletedMessageIds.Count;
            }
        }
    }
}