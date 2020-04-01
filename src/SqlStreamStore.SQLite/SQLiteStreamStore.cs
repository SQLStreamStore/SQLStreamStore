namespace SqlStreamStore
{
    using System;
    using System.Collections.Generic;
    using System.Data.SQLite;
    using System.Threading;
    using System.Threading.Tasks;
    using SqlStreamStore.Infrastructure;
    using SqlStreamStore.Logging;
    using SqlStreamStore.SQLiteScripts;
    using SqlStreamStore.Subscriptions;

    public partial class SQLiteStreamStore : StreamStoreBase
    {
        private readonly SQLiteStreamStoreSettings _settings;
        private readonly Func<SQLiteConnection> _createConnection;
        private readonly Schema _schema;
        private readonly Scripts _scripts;
        private readonly Lazy<IStreamStoreNotifier> _streamStoreNotifier;

        public const int CurrentVersion = 1;

        public SQLiteStreamStore(SQLiteStreamStoreSettings settings)
            : base(settings.GetUtcNow, settings.LogName)
        {
            _settings = settings;
            _createConnection = () => _settings.ConnectionFactory(_settings.ConnectionString);
            _streamStoreNotifier = new Lazy<IStreamStoreNotifier>(() => 
            {
                if (_settings.CreateStreamStoreNotifier == null)
                {
                    throw new InvalidOperationException(
                        "Cannot create notifier because supplied createStreamStoreNotifier was null");
                }

                return settings.CreateStreamStoreNotifier.Invoke(this);
            });
            _schema = new Schema(_settings.Schema);
            _scripts = new Scripts(_settings.Schema);
        }

        private async Task<SQLiteConnection> OpenConnection(CancellationToken cancellationToken)
        {
            var connection = _createConnection();

            await connection.OpenAsync(cancellationToken).NotOnCapturedContext();

            // any setup logic for the sqlite store.

            return connection;
        }

        public async Task CreateSchemaIfNotExists(CancellationToken cancellationToken)
        {
            using(var connection = _createConnection())
            {
                await connection.OpenAsync(cancellationToken).NotOnCapturedContext();
                using(var transaction = connection.BeginTransaction())
                {
                    using (var command = BuildCommand(_schema.Definition, transaction))
                    {
                        await command.ExecuteNonQueryAsync(cancellationToken).NotOnCapturedContext();
                    }

                    transaction.Commit();
                }
            }
        }

        private static SQLiteCommand BuildCommand(
            string commandText,
            SQLiteTransaction transaction,
            params SQLiteParameter[] parameters)
        {
            var command = new SQLiteCommand(commandText, transaction.Connection, transaction);

            foreach (var parameter in parameters)
            {
                command.Parameters.Add(parameter);
            }

            return command;
        }

        internal async Task<int> TryScavange(
            StreamIdInfo streamIdInfo,
            CancellationToken cancellationToken)
        {
            if (streamIdInfo.SQLiteStreamId == SQLiteStreamId.Deleted)
            {
                return -1;
            }

            try
            {
                using (var connection = await OpenConnection(cancellationToken))
                using (var transaction = connection.BeginTransaction())
                {
                    var deletedMessageIds = new List<Guid>();
                    using (var command = BuildCommand(
                        _scripts.Scavenge,
                        transaction, 
                        Parameters.StreamId(streamIdInfo.SQLiteStreamId)))
                    {
                        using(var reader = await command.ExecuteReaderAsync(cancellationToken).NotOnCapturedContext())
                        {
                            if (await reader.ReadAsync(cancellationToken).NotOnCapturedContext())
                            {
                                deletedMessageIds.Add(reader.GetGuid(0));
                            }
                        }
                        
                        Logger.Info(
                            "Found {count} message(s) for stream {streamId} to scavenge.",
                            deletedMessageIds.Count,
                            streamIdInfo.SQLiteStreamId);

                        if (deletedMessageIds.Count > 0)
                        {
                            Logger.Debug(
                                "Scavenging the following messages on stream {streamId}: {messageIds}",
                                streamIdInfo.SQLiteStreamId,
                                deletedMessageIds);

                            await DeleteEventsInternal(
                                streamIdInfo,
                                deletedMessageIds.ToArray(),
                                transaction,
                                cancellationToken).NotOnCapturedContext();
                        }

                        transaction.Commit();

                        return deletedMessageIds.Count;
                    }
                }
            }
            catch (Exception ex)
            {
                Logger.WarnException(
                    "Scavenge attempt failed on stream {streamId}. Another attempt will be made when this stream is written to.",
                    ex,
                    streamIdInfo.SQLiteStreamId.IdOriginal);
            }

            return -1;
        }

        protected override async Task<long> ReadHeadPositionInternal(CancellationToken cancellationToken)
        {
            GuardAgainstDisposed();

            using(var connection = _createConnection())
            {
                await connection.OpenAsync(cancellationToken).NotOnCapturedContext();

                using(var command = new SQLiteCommand("SELECT MAX(messages.position) FROM", connection))
                {
                    var result = await command
                        .ExecuteScalarAsync(cancellationToken)
                        .NotOnCapturedContext();

                    if(result == DBNull.Value)
                    {
                        return -1;
                    }
                    return (long) result;
                }
            }
        }
    }
}