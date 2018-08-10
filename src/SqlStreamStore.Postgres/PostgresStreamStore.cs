namespace SqlStreamStore
{
    using System;
    using System.Collections.Generic;
    using System.Data;
    using System.Threading;
    using System.Threading.Tasks;
    using Npgsql;
    using SqlStreamStore.Infrastructure;
    using SqlStreamStore.Logging;
    using SqlStreamStore.PgSqlScripts;
    using SqlStreamStore.Subscriptions;

    public partial class PostgresStreamStore : StreamStoreBase
    {
        private readonly PostgresStreamStoreSettings _settings;
        private readonly Func<NpgsqlConnection> _createConnection;
        private readonly Schema _schema;
        private readonly Lazy<IStreamStoreNotifier> _streamStoreNotifier;

        public const int CurrentVersion = 1;

        public PostgresStreamStore(PostgresStreamStoreSettings settings)
            : base(
                settings.GetUtcNow,
                settings.LogName)
        {
            _settings = settings;
            _createConnection = () => new NpgsqlConnection(settings.ConnectionString);
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

        private async Task<NpgsqlConnection> OpenConnection(CancellationToken cancellationToken)
        {
            var connection = _createConnection();

            await connection.OpenAsync(cancellationToken).NotOnCapturedContext();

            connection.ReloadTypes();

            connection.TypeMapper.MapComposite<PostgresNewStreamMessage>(_schema.NewStreamMessage);
            
            if(_settings.ExplainAnalyze)
            {
                using(var command = new NpgsqlCommand(_schema.EnableExplainAnalyze, connection))
                {
                    await command.ExecuteNonQueryAsync(cancellationToken).NotOnCapturedContext();
                }
            }

            return connection;
        }

        public async Task CreateSchema(CancellationToken cancellationToken = default)
        {
            using(var connection = _createConnection())
            {
                await connection.OpenAsync(cancellationToken).NotOnCapturedContext();
                using(var transaction = connection.BeginTransaction())
                {
                    using(var command = BuildCommand($"CREATE SCHEMA IF NOT EXISTS {_settings.Schema}", transaction))
                    {
                        await command.ExecuteNonQueryAsync(cancellationToken).NotOnCapturedContext();
                    }

                    using(var command = BuildCommand(_schema.Definition, transaction))
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
            throw new NotSupportedException();
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
                using(var transaction = connection.BeginTransaction())
                using(var command = BuildFunctionCommand(
                    _schema.ReadStreamMessageBeforeCreatedCount,
                    transaction,
                    Parameters.StreamId(streamIdInfo.PostgresqlStreamId),
                    Parameters.CreatedUtc(createdBefore)))
                {
                    var result = await command.ExecuteScalarAsync(cancellationToken).NotOnCapturedContext();

                    return (int) result;
                }
            }
        }

        public async Task DropAll(CancellationToken cancellationToken = default)
        {
            GuardAgainstDisposed();

            using(var connection = _createConnection())
            {
                await connection.OpenAsync(cancellationToken).NotOnCapturedContext();
                using(var transaction = connection.BeginTransaction())
                using(var command = BuildCommand(_schema.DropAll, transaction))
                {
                    await command
                        .ExecuteNonQueryAsync(cancellationToken)
                        .NotOnCapturedContext();

                    await transaction.CommitAsync(cancellationToken).NotOnCapturedContext();
                }
            }
        }

        public async Task<CheckSchemaResult> CheckSchema(CancellationToken cancellationToken = default)
        {
            using(var connection = _createConnection())
            {
                await connection.OpenAsync(cancellationToken).NotOnCapturedContext();
                using(var transaction = connection.BeginTransaction())
                using(var command = BuildFunctionCommand(_schema.ReadSchemaVersion, transaction))
                {
                    var result = (int) await command.ExecuteScalarAsync(cancellationToken).NotOnCapturedContext();

                    return new CheckSchemaResult(result, CurrentVersion);
                }
            }
        }

        private Func<CancellationToken, Task<string>> GetJsonData(PostgresqlStreamId streamId, int version)
            => async cancellationToken =>
            {
                using(var connection = await OpenConnection(cancellationToken))
                using(var transaction = connection.BeginTransaction())
                using(var command = BuildFunctionCommand(
                    _schema.ReadJsonData,
                    transaction,
                    Parameters.StreamId(streamId),
                    Parameters.Version(version)))
                {
                    var jsonData = await command.ExecuteScalarAsync(cancellationToken).NotOnCapturedContext();

                    return jsonData == DBNull.Value ? null : (string) jsonData;
                }
            };

        private static NpgsqlCommand BuildFunctionCommand(
            string function,
            NpgsqlTransaction transaction,
            params NpgsqlParameter[] parameters)
        {
            var command = new NpgsqlCommand(function, transaction.Connection, transaction)
            {
                CommandType = CommandType.StoredProcedure
            };

            foreach(var parameter in parameters)
            {
                command.Parameters.Add(parameter);
            }

            return command;
        }

        private static NpgsqlCommand BuildCommand(
            string commandText,
            NpgsqlTransaction transaction) => new NpgsqlCommand(commandText, transaction.Connection, transaction);

        internal async Task<int> TryScavenge(
            StreamIdInfo streamIdInfo,
            int? maxCount,
            CancellationToken cancellationToken)
        {
            if(streamIdInfo.PostgresqlStreamId == PostgresqlStreamId.Deleted)
            {
                return -1;
            }

            try
            {
                using(var connection = await OpenConnection(cancellationToken))
                using(var transaction = connection.BeginTransaction())
                {
                    var deletedMessageIds = new List<Guid>();
                    using(var command = BuildFunctionCommand(
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
            catch(Exception ex)
            {
                if(Logger.IsWarnEnabled())
                {
                    Logger.WarnException(
                        $"Scavenge attempt failed on stream {streamIdInfo.PostgresqlStreamId.IdOriginal}. Another attempt will be made when this stream is written to.",
                        ex);
                }
            }

            return -1;
        }
    }
}