namespace SqlStreamStore
{
    using System;
    using System.Data;
    using System.Threading;
    using System.Threading.Tasks;
    using MySql.Data.MySqlClient;
    using SqlStreamStore.Imports.Ensure.That;
    using SqlStreamStore.Infrastructure;
    using SqlStreamStore.MySqlScripts;
    using SqlStreamStore.Streams;
    using SqlStreamStore.Subscriptions;

    /// <inheritdoc />
    /// <summary>
    ///     Represents a MySql stream store implementation.
    /// </summary>
    public sealed partial class MySqlStreamStore : StreamStoreBase
    {
        private readonly Func<MySqlConnection> _createConnection;
        private readonly Lazy<IStreamStoreNotifier> _streamStoreNotifier;
        private readonly Scripts _scripts;
        public const int FirstSchemaVersion = 1;
        public const int CurrentSchemaVersion = 2;

        /// <inheritdoc />
        /// <summary>
        ///     Initializes a new instance of <see cref="T:SqlStreamStore.MySqlStreamStore" />
        /// </summary>
        /// <param name="settings">A settings class to configure this instance.</param>
        public MySqlStreamStore(MySqlStreamStoreSettings settings)
            :base(settings.MetadataMaxAgeCacheExpire, settings.MetadataMaxAgeCacheMaxSize,
                 settings.GetUtcNow, settings.LogName)
        {
            Ensure.That(settings, nameof(settings)).IsNotNull();

            _createConnection = () => new MySqlConnection(settings.ConnectionString);
            _streamStoreNotifier = new Lazy<IStreamStoreNotifier>(() =>
                {
                    if(settings.CreateStreamStoreNotifier == null)
                    {
                        throw new InvalidOperationException(
                            "Cannot create notifier because supplied createStreamStoreNotifier was null");
                    }
                    return settings.CreateStreamStoreNotifier.Invoke(this);
                });
            _scripts = new Scripts();
        }

        /// <summary>
        ///     Creates a database to hold streams
        /// </summary>
        /// <param name="cancellationToken">The cancellation instruction.</param>
        /// <returns>A <see cref="Task"/> representing the asynchronous operation.</returns>
        public async Task CreateDatabase(CancellationToken cancellationToken = default(CancellationToken))
        {
            GuardAgainstDisposed();

            using(var connection = _createConnection())
            {
                await connection.OpenAsync(cancellationToken).NotOnCapturedContext();

                using(var transaction = await connection.BeginTransactionAsync(cancellationToken))
                {
                    using(var command = new MySqlCommand(_scripts.CreateDatabase, connection, transaction))
                    {
                        await command
                            .ExecuteNonQueryAsync(cancellationToken)
                            .NotOnCapturedContext();
                    }

                    if(!await IndexExists("IX_Streams_Id", "Streams", connection, transaction, cancellationToken))
                    {
                        using(var command = new MySqlCommand(_scripts.IndexStreamsById, connection, transaction))
                        {
                            await command
                                .ExecuteNonQueryAsync(cancellationToken)
                                .NotOnCapturedContext();
                        }
                    }

                    if(!await IndexExists("IX_Messages_Position", "Messages", connection, transaction, cancellationToken))
                    {
                        using(var command = new MySqlCommand(_scripts.IndexMessagesByPosition, connection, transaction))
                        {
                            await command
                                .ExecuteNonQueryAsync(cancellationToken)
                                .NotOnCapturedContext();
                        }
                    }

                    if(!await IndexExists(
                        "IX_Messages_StreamIdInternal_Id",
                        "Messages",
                        connection,
                        transaction,
                        cancellationToken))
                    {
                        using(var command = new MySqlCommand(_scripts.IndexMessagesByStreamIdInternalAndId,
                            connection,
                            transaction))
                        {
                            await command
                                .ExecuteNonQueryAsync(cancellationToken)
                                .NotOnCapturedContext();
                        }
                    }

                    if(!await IndexExists(
                        "IX_Messages_StreamIdInternal_Created",
                        "Messages",
                        connection,
                        transaction,
                        cancellationToken))
                    {
                        using(var command = new MySqlCommand(_scripts.IndexMessagesByStreamIdInternalAndCreated,
                            connection,
                            transaction))
                        {
                            await command
                                .ExecuteNonQueryAsync(cancellationToken)
                                .NotOnCapturedContext();
                        }
                    }

                    if(!await IndexExists(
                        "IX_Messages_StreamIdInternal_Revision",
                        "Messages",
                        connection,
                        transaction,
                        cancellationToken))
                    {
                        using(var command = new MySqlCommand(_scripts.IndexMessagesByStreamIdInternalAndStreamVersion, connection, transaction))
                        {
                            await command
                                .ExecuteNonQueryAsync(cancellationToken)
                                .NotOnCapturedContext();
                        }
                    }
                    await transaction.CommitAsync(cancellationToken);
                }
            }
        }

        /// <summary>
        ///     Drops all tables related to this store instance.
        /// </summary>
        /// <param name="cancellationToken">The cancellation instruction.</param>
        /// <returns>A <see cref="Task"/> representing the asynchronous operation.</returns>
        public async Task DropAll(CancellationToken cancellationToken = default(CancellationToken))
        {
            GuardAgainstDisposed();

            using(var connection = _createConnection())
            {
                await connection.OpenAsync(cancellationToken).NotOnCapturedContext();

                using(var command = new MySqlCommand(_scripts.DropAll, connection))
                {
                    await command
                        .ExecuteNonQueryAsync(cancellationToken)
                        .NotOnCapturedContext();
                }
            }
        }

        /// <inheritdoc />
        protected override async Task<int> GetStreamMessageCount(
            string streamId,
            CancellationToken cancellationToken = default(CancellationToken))
        {
            GuardAgainstDisposed();

            using(var connection = _createConnection())
            {
                await connection.OpenAsync(cancellationToken).NotOnCapturedContext();

                using(var command = new MySqlCommand(_scripts.GetStreamMessageCount, connection))
                {
                    var streamIdInfo = new StreamIdInfo(streamId);
                    command.Parameters.AddWithValue("streamId", streamIdInfo.SqlStreamId.Id);

                    var result = await command
                        .ExecuteScalarAsync(cancellationToken)
                        .NotOnCapturedContext();

                    return Convert.ToInt32((long) result);
                }
            }
        }

        public async Task<int> GetMessageCount(
            string streamId,
            DateTime createdBefore,
            CancellationToken cancellationToken = default(CancellationToken))
        {
            GuardAgainstDisposed();

            using (var connection = _createConnection())
            {
                await connection.OpenAsync(cancellationToken).NotOnCapturedContext();

                using (var command = new MySqlCommand(_scripts.GetStreamMessageBeforeCreatedCount, connection))
                {
                    var streamIdInfo = new StreamIdInfo(streamId);
                    command.Parameters.AddWithValue("streamId", streamIdInfo.SqlStreamId.Id);
                    command.Parameters.AddWithValue("created", createdBefore.Ticks);

                    var result = await command
                        .ExecuteScalarAsync(cancellationToken)
                        .NotOnCapturedContext();

                    return Convert.ToInt32((long) result);
                }
            }
        }

        protected override async Task<long> ReadHeadPositionInternal(CancellationToken cancellationToken)
        {
            GuardAgainstDisposed();

            using(var connection = _createConnection())
            {
                await connection.OpenAsync(cancellationToken).NotOnCapturedContext();

                using(var command = new MySqlCommand(_scripts.ReadHeadPosition, connection))
                {
                    var result = await command
                        .ExecuteScalarAsync(cancellationToken)
                        .NotOnCapturedContext();

                    return result == DBNull.Value 
                        ? Position.End 
                        : MySqlOrdinal.CreateFromMySqlOrdinal((long) result).ToStreamStorePosition();
                }
            }
        }

        private async Task<bool> IndexExists(
            string indexName,
            string tableName,
            MySqlConnection connection,
            MySqlTransaction transaction = null,
            CancellationToken cancellationToken = default(CancellationToken))
        {
            using(var command = new MySqlCommand(connection, transaction)
            {
                CommandText = _scripts.IndexExists,
                CommandType = CommandType.Text
            })
            {
                command.Parameters.AddWithValue(nameof(indexName), indexName);
                command.Parameters.AddWithValue(nameof(tableName), tableName);

                var result = await command.ExecuteScalarAsync(cancellationToken)
                    .NotOnCapturedContext();

                return (long) result == 1;
            }
        }

        private async Task<int?> GetStreamIdInternal(
            MySqlStreamId streamId,
            MySqlConnection connection,
            MySqlTransaction transaction,
            CancellationToken cancellationToken)
        {
            using(var command = new MySqlCommand(_scripts.GetStreamIdInternal, connection, transaction))
            {
                command.Parameters.AddWithValue("streamId", streamId.Id);

                var streamIdInternal = await command.ExecuteScalarAsync(cancellationToken);

                return (int?) streamIdInternal;
            }
        }

        private async Task<int?> GetLatestStreamVersion(
            int streamIdInternal,
            MySqlConnection connection,
            MySqlTransaction transaction,
            CancellationToken cancellationToken)
        {
            using(var command = new MySqlCommand(_scripts.GetLatestStreamVersion, connection, transaction))
            {
                command.Parameters.AddWithValue("streamIdInternal", streamIdInternal);

                var latestStreamVersion = await command.ExecuteScalarAsync(cancellationToken);

                return (int?) latestStreamVersion;
            }
        }

        private async Task CheckStreamMaxCount(string streamId, int? maxCount, CancellationToken cancellationToken)
        {
            if (maxCount.HasValue)
            {
                var count = await GetStreamMessageCount(streamId, cancellationToken);
                if (count > maxCount.Value)
                {
                    int toPurge = count - maxCount.Value;

                    var streamMessagesPage = await ReadStreamForwardsInternal(streamId, StreamVersion.Start,
                        toPurge, false, null, cancellationToken);

                    if (streamMessagesPage.Status == PageReadStatus.Success)
                    {
                        foreach (var message in streamMessagesPage.Messages)
                        {
                            await DeleteEventInternal(streamId, message.MessageId, cancellationToken);
                        }
                    }
                }
            }
        }

        private DateTime UtcNow => DateTime.SpecifyKind(GetUtcNow(), DateTimeKind.Utc);

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

        private IObservable<Unit> GetStoreObservable => _streamStoreNotifier.Value;
    }
}