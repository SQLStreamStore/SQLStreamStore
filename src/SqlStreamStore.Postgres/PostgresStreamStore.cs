﻿namespace SqlStreamStore
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

    /// <summary>
    ///     Represents a PostgreSQL stream store implementation.
    /// </summary>
    public partial class PostgresStreamStore : StreamStoreBase
    {
        private readonly PostgresStreamStoreSettings _settings;
        private readonly Func<NpgsqlConnection> _createConnection;
        private readonly Schema _schema;
        private readonly Lazy<IStreamStoreNotifier> _streamStoreNotifier;

        public const int CurrentVersion = 1;

        /// <summary>
        ///     Initializes a new instance of <see cref="PostgresStreamStore"/>
        /// </summary>
        /// <param name="settings">A settings class to configure this instance.</param>
        public PostgresStreamStore(PostgresStreamStoreSettings settings)
            : base(settings.GetUtcNow, settings.LogName)
        {
            _settings = settings;
            _createConnection = () => _settings.ConnectionFactory(_settings.ConnectionString);
            _streamStoreNotifier = new Lazy<IStreamStoreNotifier>(() =>
            {
                if(_settings.CreateStreamStoreNotifier == null)
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

        /// <summary>
        ///     Creates a scheme that will hold streams and messages, if the schema does not exist.
        ///     Calls to this should part of an application's deployment/upgrade process and
        ///     not every time your application boots up.
        /// </summary>
        /// <param name="cancellationToken">The cancellation instruction.</param>
        /// <returns>A <see cref="Task"/> representing the asynchronous operation.</returns>
        public async Task CreateSchemaIfNotExists(CancellationToken cancellationToken = default)
        {
            using(var connection = _createConnection())
            {
                await connection.OpenAsync(cancellationToken).NotOnCapturedContext();
                using(var transaction = connection.BeginTransaction())
                {
                    using(var command = BuildCommand(_schema.Definition, transaction))
                    {
                        await command.ExecuteNonQueryAsync(cancellationToken).NotOnCapturedContext();
                    }

                    await transaction.CommitAsync(cancellationToken).NotOnCapturedContext();
                }
            }
        }

        /// <summary>
        ///     Drops all tables related to this store instance.
        /// </summary>
        /// <param name="cancellationToken">The cancellation instruction.</param>
        /// <returns>A <see cref="Task"/> representing the asynchronous operation.</returns>
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

        /// <summary>
        ///     Checks the store schema for the correct version.
        /// </summary>
        /// <param name="cancellationToken"></param>
        /// <returns>A <see cref="CheckSchemaResult"/> representing the result of the operation.</returns>
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
                using(var reader = await command
                    .ExecuteReaderAsync(CommandBehavior.SequentialAccess, cancellationToken)
                    .NotOnCapturedContext())
                {
                    if(!await reader.ReadAsync(cancellationToken).NotOnCapturedContext() || reader.IsDBNull(0))
                    {
                        return null;
                    }

                    using(var textReader = reader.GetTextReader(0))
                    {
                        return await textReader.ReadToEndAsync().NotOnCapturedContext();
                    }
                }
            };

        private static NpgsqlCommand BuildFunctionCommand(
            string function,
            NpgsqlTransaction transaction,
            params NpgsqlParameter[] parameters)
        {
            var command = new NpgsqlCommand(function, transaction.Connection, transaction)
            {
                CommandType = CommandType.StoredProcedure,
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
                        Parameters.StreamId(streamIdInfo.PostgresqlStreamId)))
                    using(var reader = await command
                        .ExecuteReaderAsync(cancellationToken)
                        .NotOnCapturedContext())
                    {
                        while(await reader.ReadAsync(cancellationToken).NotOnCapturedContext())
                        {
                            deletedMessageIds.Add(reader.GetGuid(0));
                        }
                    }

                    Logger.Info(
                        "Found {count} message(s) for stream {streamId} to scavenge.",
                        deletedMessageIds.Count,
                        streamIdInfo.PostgresqlStreamId);

                    if(deletedMessageIds.Count > 0)
                    {
                        Logger.Debug(
                            "Scavenging the following messages on stream {streamId}: {messageIds}",
                            streamIdInfo.PostgresqlStreamId,
                            deletedMessageIds);

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
                Logger.WarnException(
                    "Scavenge attempt failed on stream {streamId}. Another attempt will be made when this stream is written to.",
                    ex,
                    streamIdInfo.PostgresqlStreamId.IdOriginal);
            }

            return -1;
        }

        /// <summary>
        /// Returns the script that can be used to create the Sql Stream Store in a Postgres database.
        /// </summary>
        /// <returns>The database creation script.</returns>
        public string GetSchemaCreationScript()
        {
            return _schema.Definition;
        }
    }
}