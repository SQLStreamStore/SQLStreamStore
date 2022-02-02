namespace SqlStreamStore
{
    using System;
    using System.Collections.Generic;
    using System.Data;
    using System.Linq;
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
    public partial class PostgresStreamStore : StreamStoreBase<PostgresReadAllPage>
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

            await connection.OpenAsync(cancellationToken).ConfigureAwait(false);

            connection.ReloadTypes();

            connection.TypeMapper.MapComposite<PostgresNewStreamMessage>(_schema.NewStreamMessage);

            if(_settings.ExplainAnalyze)
            {
                using(var command = new NpgsqlCommand(_schema.EnableExplainAnalyze, connection))
                {
                    await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
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
                await connection.OpenAsync(cancellationToken).ConfigureAwait(false);
                using(var transaction = connection.BeginTransaction())
                {
                    using(var command = BuildCommand($"CREATE SCHEMA IF NOT EXISTS {_settings.Schema}", transaction))
                    {
                        await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
                    }

                    using(var command = BuildCommand(_schema.Definition, transaction))
                    {
                        await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
                    }

                    await transaction.CommitAsync(cancellationToken).ConfigureAwait(false);
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
                await connection.OpenAsync(cancellationToken).ConfigureAwait(false);
                using(var transaction = connection.BeginTransaction())
                using(var command = BuildCommand(_schema.DropAll, transaction))
                {
                    await command
                        .ExecuteNonQueryAsync(cancellationToken)
                        .ConfigureAwait(false);

                    await transaction.CommitAsync(cancellationToken).ConfigureAwait(false);
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
                await connection.OpenAsync(cancellationToken).ConfigureAwait(false);
                using(var transaction = connection.BeginTransaction())
                using(var command = BuildFunctionCommand(_schema.ReadSchemaVersion, transaction))
                {
                    var result = (int) await command.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false);

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
                    .ConfigureAwait(false))
                {
                    if(!await reader.ReadAsync(cancellationToken).ConfigureAwait(false) || reader.IsDBNull(0))
                    {
                        return null;
                    }

                    using(var textReader = reader.GetTextReader(0))
                    {
                        return await textReader.ReadToEndAsync().ConfigureAwait(false);
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
                        .ConfigureAwait(false))
                    {
                        while(await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
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
                            cancellationToken).ConfigureAwait(false);
                    }

                    await transaction.CommitAsync(cancellationToken).ConfigureAwait(false);

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

        //protected override async Task<PostgresReadAllPage> HandleGap(
        //    PostgresReadAllPage page,
        //    long fromPositionInclusive,
        //    int maxCount,
        //    bool prefetchJsonData,
        //    CancellationToken cancellationToken)
        //{
        //    if (page.Messages.Length == 0 || DateTime.UtcNow - page.Messages[page.Messages.Length - 1].CreatedUtc > TimeSpan.FromMinutes(5))
        //        return page;

        //    // TODO: FIXIT
        //    // Check for gap between last page and this.
        //    if (page.Messages[0].Position != fromPositionInclusive)
        //    {
        //        Logger.DebugFormat("Gap detected at lower page boundary. Potentially could have lost {lostMessageCount} events if the gap is transient", page.Messages[0].Position - fromPositionInclusive);
        //        page = await HandleGap(page, fromPositionInclusive, maxCount, prefetchJsonData, cancellationToken);
        //        //if (!page.IsEnd || page.Messages.Length == 1)
        //        //    Logger.DebugFormat("Gap detected at lower page boundary.  Potentially could have lost {lostMessageCount} events if the gap is transient", page.Messages[0].Position - fromPositionInclusive);
        //        //page = await ReloadAfterDelay(fromPositionInclusive, maxCount, prefetchJsonData, ReadNext, cancellationToken);
        //    }

        //    // check for gap in messages collection
        //    for (int i = 0; i < page.Messages.Length - 1; i++)
        //    {
        //        var expectedNextPosition = page.Messages[i].Position + 1;
        //        if (expectedNextPosition != page.Messages[i + 1].Position)
        //        {
        //            Logger.InfoFormat("Gap detected in " + (page.IsEnd ? "last" : "(NOT the last)") + " page.  Returning partial page {fromPosition}-{toPosition}", fromPositionInclusive, fromPositionInclusive + i + 1);

        //            PostgresReadAllPage requeryPage;
        //            var maxPosition = page.Messages[page.Messages.Length - 1].Position;
        //            do
        //            {
        //                requeryPage = await ReadAllForwardsInternal(fromPositionInclusive, maxCount, prefetchJsonData, cancellationToken, maxPosition);
        //            } while (page.TransactionIds.Intersect(requeryPage.TransactionIds).Any());

        //            return requeryPage;

        //            // switched this to return the partial page, then re-issue load starting at gap
        //            // this speeds up the retry instead of taking a 3 second delay immediately
        //            //var messagesBeforeGap = new StreamMessage[i+1];
        //            //page.Messages.Take(i+1).ToArray().CopyTo(messagesBeforeGap, 0);
        //            //return new ReadAllPage(page.FromPosition, maxPosition, page.IsEnd, page.Direction, ReadNext, messagesBeforeGap);
        //        }
        //    }

        //    //ReadAllPage requeryPage;
        //    //var maxPosition = pageWithGap.Messages[pageWithGap.Messages.Length - 1].Position;
        //    //do
        //    //{
        //    //    requeryPage = await ReadAllForwardsInternal(fromPositionInclusive, maxCount, prefetchJsonData, readNext, cancellationToken, maxPosition);
        //    //} while (pageWithGap.TxSnapshot.CurrentTxIds.Intersect(requeryPage.TxSnapshot.CurrentTxIds).Any());

        //    return page;
        //}

        //protected override async Task<T> HandleGap<T>(T page, long fromPositionInclusive, int maxCount, bool prefetchJsonData, ReadNextAllPage readNext, CancellationToken cancellationToken) where T : PostgresReadAllPage
        //{
        //    if (page.Messages.Length == 0 || DateTime.UtcNow - page.Messages[page.Messages.Length - 1].CreatedUtc > TimeSpan.FromMinutes(5))
        //        return page;

        //    // TODO: FIXIT
        //    // Check for gap between last page and this.
        //    if (page.Messages[0].Position != fromPositionInclusive)
        //    {
        //        Logger.DebugFormat("Gap detected at lower page boundary. Potentially could have lost {lostMessageCount} events if the gap is transient", page.Messages[0].Position - fromPositionInclusive);
        //        page = await HandleGap(page, fromPositionInclusive, maxCount, prefetchJsonData, readNext, cancellationToken);
        //        //if (!page.IsEnd || page.Messages.Length == 1)
        //        //    Logger.DebugFormat("Gap detected at lower page boundary.  Potentially could have lost {lostMessageCount} events if the gap is transient", page.Messages[0].Position - fromPositionInclusive);
        //        //page = await ReloadAfterDelay(fromPositionInclusive, maxCount, prefetchJsonData, ReadNext, cancellationToken);
        //    }

        //    // check for gap in messages collection
        //    for (int i = 0; i < page.Messages.Length - 1; i++)
        //    {
        //        var expectedNextPosition = page.Messages[i].Position + 1;
        //        if (expectedNextPosition != page.Messages[i + 1].Position)
        //        {
        //            Logger.InfoFormat("Gap detected in " + (page.IsEnd ? "last" : "(NOT the last)") + " page.  Returning partial page {fromPosition}-{toPosition}", fromPositionInclusive, fromPositionInclusive + i + 1);

        //            ReadAllPage requeryPage;
        //            var maxPosition = page.Messages[page.Messages.Length - 1].Position;
        //            do
        //            {
        //                requeryPage = await ReadAllForwardsInternal(fromPositionInclusive, maxCount, prefetchJsonData, readNext, cancellationToken, maxPosition);
        //            } while (page.TransactionIds.Intersect(requeryPage.TransactionIds).Any());

        //            return requeryPage;

        //            // switched this to return the partial page, then re-issue load starting at gap
        //            // this speeds up the retry instead of taking a 3 second delay immediately
        //            //var messagesBeforeGap = new StreamMessage[i+1];
        //            //page.Messages.Take(i+1).ToArray().CopyTo(messagesBeforeGap, 0);
        //            //return new ReadAllPage(page.FromPosition, maxPosition, page.IsEnd, page.Direction, ReadNext, messagesBeforeGap);
        //        }
        //    }

        //    //ReadAllPage requeryPage;
        //    //var maxPosition = pageWithGap.Messages[pageWithGap.Messages.Length - 1].Position;
        //    //do
        //    //{
        //    //    requeryPage = await ReadAllForwardsInternal(fromPositionInclusive, maxCount, prefetchJsonData, readNext, cancellationToken, maxPosition);
        //    //} while (pageWithGap.TxSnapshot.CurrentTxIds.Intersect(requeryPage.TxSnapshot.CurrentTxIds).Any());

        //    return page;
        //}
    }
}
