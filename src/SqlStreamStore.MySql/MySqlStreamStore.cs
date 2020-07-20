namespace SqlStreamStore
{
    using System;
    using System.Collections.Generic;
    using System.Data;
    using System.Threading;
    using System.Threading.Tasks;
    using MySqlConnector;
    using SqlStreamStore.Infrastructure;
    using SqlStreamStore.Logging;
    using SqlStreamStore.MySqlScripts;
    using SqlStreamStore.Subscriptions;
    using StreamStoreStore.Json;

    /// <inheritdoc />
    /// <summary>
    ///     Represents a MySql stream store implementation.
    /// </summary>
    public partial class MySqlStreamStore : StreamStoreBase
    {
        private readonly MySqlStreamStoreSettings _settings;
        private readonly Func<MySqlConnection> _createConnection;
        private readonly Schema _schema;
        private readonly Lazy<IStreamStoreNotifier> _streamStoreNotifier;

        public const int CurrentVersion = 1;

        /// <inheritdoc />
        /// <summary>
        ///     Initializes a new instance of <see cref="T:SqlStreamStore.MySqlStreamStore" />
        /// </summary>
        /// <param name="settings">A settings class to configure this instance.</param>
        public MySqlStreamStore(MySqlStreamStoreSettings settings)
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
            _schema = new Schema();
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
            GuardAgainstDisposed();

            using(var connection = await OpenConnection(cancellationToken))
            using(var transaction = await connection.BeginTransactionAsync(cancellationToken).ConfigureAwait(false))
            {
                using(var command = BuildCommand(_schema.Definition, transaction))
                {
                    await command.ExecuteNonQueryAsync(cancellationToken).NotOnCapturedContext();
                }

                await transaction.CommitAsync(cancellationToken).NotOnCapturedContext();
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
                using(var transaction = await connection.BeginTransactionAsync(cancellationToken).ConfigureAwait(false))
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
            GuardAgainstDisposed();

            using(var connection = _createConnection())
            {
                await connection.OpenAsync(cancellationToken).NotOnCapturedContext();
                using(var transaction = await connection.BeginTransactionAsync(cancellationToken).ConfigureAwait(false))
                using(var command = BuildStoredProcedureCall(_schema.ReadProperties, transaction))
                {
                    var properties = SimpleJson.DeserializeObject<SchemaProperties>(
                        (string) await command.ExecuteScalarAsync(cancellationToken).NotOnCapturedContext());

                    return new CheckSchemaResult(properties.version, CurrentVersion);
                }
            }
        }

        protected override void Dispose(bool disposing)
        {
            if(disposing)
            {
                if(_streamStoreNotifier.IsValueCreated)
                {
                    _streamStoreNotifier.Value.Dispose();
                }
            }

            base.Dispose(disposing);
        }

        internal async Task<int> TryScavenge(
            StreamIdInfo streamId,
            CancellationToken cancellationToken)
        {
            if(streamId.MySqlStreamId == MySqlStreamId.Deleted)
            {
                return -1;
            }

            try
            {
                using(var connection = await OpenConnection(cancellationToken))
                using(var transaction = await connection.BeginTransactionAsync(cancellationToken).ConfigureAwait(false))
                {
                    var deletedMessageIds = new List<Guid>();
                    using(var command = BuildStoredProcedureCall(
                        _schema.Scavenge,
                        transaction,
                        Parameters.StreamId(streamId.MySqlStreamId)))
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
                        "Found {deletedMessageIdCount} message(s) for stream {streamId} to scavenge.",
                        deletedMessageIds.Count,
                        streamId.MySqlStreamId.IdOriginal);

                    if(deletedMessageIds.Count > 0)
                    {
                        Logger.Debug(
                            "Scavenging the following messages on stream {streamId}: {deletedMessageIds}",
                            streamId.MySqlStreamId.IdOriginal,
                            deletedMessageIds);
                    }

                    foreach(var deletedMessageId in deletedMessageIds)
                    {
                        await DeleteEventInternal(streamId, deletedMessageId, transaction, cancellationToken);
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
                    streamId.MySqlStreamId.IdOriginal);
            }

            return -1;
        }

        private static MySqlCommand BuildStoredProcedureCall(
            string procedure,
            MySqlTransaction transaction,
            params MySqlParameter[] parameters)
        {
            var command = new MySqlCommand(procedure, transaction.Connection, transaction)
            {
                CommandType = CommandType.StoredProcedure
            };

            foreach(var parameter in parameters)
            {
                command.Parameters.Add(parameter);
            }

            return command;
        }

        private static MySqlCommand BuildCommand(
            string commandText,
            MySqlTransaction transaction) => new MySqlCommand(commandText, transaction.Connection, transaction);

        /// <summary>
        /// Returns the script that can be used to create the Sql Stream Store in a MySql database.
        /// </summary>
        /// <returns>The database creation script.</returns>
        public string GetSchemaCreationScript()
        {
            return _schema.Definition;
        }

        private async Task<MySqlConnection> OpenConnection(CancellationToken cancellationToken)
        {
            var connection = _createConnection();
            await connection.OpenAsync(cancellationToken).NotOnCapturedContext(); 
            return connection;
        }

        private Func<CancellationToken, Task<string>> GetJsonData(MySqlStreamId streamId, int version)
            => async cancellationToken =>
            {
                using(var connection = await OpenConnection(cancellationToken))
                using(var transaction = await connection.BeginTransactionAsync(cancellationToken).ConfigureAwait(false))
                using(var command = BuildStoredProcedureCall(
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

        private class SchemaProperties
        {
            public int version { get; set; }
        }
    }
}