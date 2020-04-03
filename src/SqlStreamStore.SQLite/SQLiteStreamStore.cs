namespace SqlStreamStore
{
    using System;
    using System.Collections.Generic;
    using System.Threading;
    using System.Threading.Tasks;
    using Microsoft.Data.Sqlite;
    using SqlStreamStore.Infrastructure;
    using SqlStreamStore.Logging;
    using SqlStreamStore.SQLiteScripts;
    using SqlStreamStore.Subscriptions;

    public partial class SQLiteStreamStore : StreamStoreBase
    {
        private readonly SQLiteStreamStoreSettings _settings;
        private readonly Func<SqliteConnection> _createConnection;
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

        private SqliteConnection OpenConnection()
        {
            var connection = _createConnection();

            connection.Open();
            using(var command = connection.CreateCommand())
            {
                command.CommandText = @"PRAGMA foreign_keys = true;
                                        PRAGMA journal_mode = WAL;";
                command.ExecuteNonQuery();
            }

            return connection;
        }

        public void CreateSchemaIfNotExists()
        {
            using(var connection = OpenConnection())
            using(var transaction = connection.BeginTransaction())
            {
                using(var command = connection.CreateCommand())
                {
                    command.Transaction = transaction;
                    command.CommandText = _scripts.CreateSchema;
                    command.ExecuteNonQuery();
                }

                transaction.Commit();
            }
        }

        protected override Task<long> ReadHeadPositionInternal(CancellationToken cancellationToken)
        {
            GuardAgainstDisposed();

            using(var connection = OpenConnection())
            {
                using(var command = new SqliteCommand("SELECT MAX(messages.position) FROM messages", connection))
                {
                    var result = command.ExecuteScalar();

                    return Task.FromResult(result == DBNull.Value ? -1 : (long)result);
                }
            }
        }

        internal int TryScavenge(
            StreamIdInfo streamId,
            CancellationToken cancellationToken)
        {
            if(streamId.SQLiteStreamId == SQLiteStreamId.Deleted)
            {
                return -1;
            }

            try
            {
                using(var connection = OpenConnection())
                using(var transaction = connection.BeginTransaction())
                {
                    var deletedMessageIds = new List<Guid>();
                    using(var command = connection.CreateCommand())
                    {
                        long? _stream_id_internal = 0;
                        long? _max_count = 0;
                        
                        command.Transaction = transaction;

                        command.CommandText = @"SELECT streams.id_internal, streams.max_count
                                                FROM streams WHERE streams.id = @streamId";
                        command.Parameters.Clear();
                        command.Parameters.AddWithValue("@streamId", streamId.SQLiteStreamId.Id);
                        using(var reader = command.ExecuteReader())
                        {
                            if(reader.Read())
                            {
                                _stream_id_internal = reader.GetInt64(0);
                                _max_count = reader.GetInt64(1);
                            }
                        }

                        command.CommandText = @"SELECT messages.message_id
                                                FROM messages
                                                WHERE messages.stream_id_internal = ((SELECT coalesce(RealValue, IntegerValue, BlobValue, TextValue) FROM _Variables WHERE Name = '_stream_id_internal' LIMIT 1))
                                                    AND messages.message_id NOT IN (SELECT messages.message_id
                                                                                    FROM messages
                                                                                    WHERE messages.stream_id_internal = @streamIdInternal)
                                                                                        ORDER BY messages.stream_version desc
                                                                                        LIMIT @maxCount)";
                        command.Parameters.Clear();
                        command.Parameters.AddWithValue("@internalStreamId", _stream_id_internal);
                        command.Parameters.AddWithValue("@maxCount", _max_count);

                        using(var reader = command.ExecuteReader())
                        {
                            while(reader.Read())
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
                        DeleteEventInternal(streamId, deletedMessageId, transaction, cancellationToken);
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
