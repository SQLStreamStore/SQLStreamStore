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
                if(_settings.CreateStreamStoreNotifier == null)
                {
                    throw new InvalidOperationException(
                        "Cannot create notifier because supplied createStreamStoreNotifier was null");
                }

                return settings.CreateStreamStoreNotifier.Invoke(this);
            });
            _scripts = new Scripts();
        }

        private async Task<SQLiteConnection> OpenConnection(CancellationToken cancellationToken)
        {
            var connection = _createConnection();

            await connection.OpenAsync(cancellationToken).NotOnCapturedContext();

            // any setup logic for the sqlite store.

            return connection;
        }

        public async Task CreateSchema(CancellationToken cancellationToken = default)
        {
            using(var connection = _createConnection())
            {
                await connection.OpenAsync(cancellationToken).NotOnCapturedContext();
                using(var transaction = connection.BeginTransaction())
                {
                    using(var command = connection.CreateCommand())
                    {
                        command.Transaction = transaction;
                        command.CommandText = _scripts.CreateSchema;
                        await command.ExecuteNonQueryAsync(cancellationToken).NotOnCapturedContext();
                    }

                    transaction.Commit();
                }
            }
        }

        protected override async Task<long> ReadHeadPositionInternal(CancellationToken cancellationToken)
        {
            GuardAgainstDisposed();

            using(var connection = _createConnection())
            {
                await connection.OpenAsync(cancellationToken).NotOnCapturedContext();

                using(var command = new SQLiteCommand("SELECT MAX(messages.position) FROM messages", connection))
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

        internal async Task<int> TryScavenge(
            StreamIdInfo streamId,
            CancellationToken cancellationToken)
        {
            if(streamId.SQLiteStreamId == SQLiteStreamId.Deleted)
            {
                return -1;
            }

            try
            {
                using(var connection = await OpenConnection(cancellationToken))
                using(var transaction = connection.BeginTransaction())
                {
                    var deletedMessageIds = new List<Guid>();
                    using(var command = connection.CreateCommand())
                    {
                        command.CommandText = _scripts.Scavenge;
                        command.Parameters.Clear();
                        command.Parameters.Add(new SQLiteParameter("@@streamId", streamId.SQLiteStreamId.Id));

                        using(var reader = await command.ExecuteReaderAsync(cancellationToken))
                        {
                            while(await reader.ReadAsync(cancellationToken))
                            {
                                deletedMessageIds.Add(reader.GetGuid(0));
                            }
                        }
                    }

                    Logger.Info(
                        "Found {deletedMessageIdCount} message(s) for stream {streamId} to scavenge.",
                        deletedMessageIds.Count,
                        streamId.SQLiteStreamId.IdOriginal);

                    if(deletedMessageIds.Count > 0)
                    {
                        Logger.Debug(
                            "Scavenging the following messages on stream {streamId}: {deletedMessageIds}",
                            streamId.SQLiteStreamId.IdOriginal,
                            deletedMessageIds);
                    }

                    foreach(var deletedMessageId in deletedMessageIds)
                    {
                        await DeleteEventInternal(streamId, deletedMessageId, transaction, cancellationToken);
                    }

                    transaction.Commit();

                    return deletedMessageIds.Count;
                }
            }
            catch(Exception ex)
            {
                Logger.WarnException(
                    "Scavenge attempt failed on stream {streamId}. Another attempt will be made when this stream is written to.",
                    ex,
                    streamId.SQLiteStreamId.IdOriginal);
            }

            return -1;
        }
    }
}
