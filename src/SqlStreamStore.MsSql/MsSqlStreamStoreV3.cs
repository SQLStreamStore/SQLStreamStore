namespace SqlStreamStore
{
    using System;
    using System.Collections.Generic;
    using System.Data;
    using System.Threading;
    using System.Threading.Tasks;
    using Microsoft.Data.SqlClient;
    using Microsoft.Data.SqlClient.Server;
    using SqlStreamStore.Imports.Ensure.That;
    using SqlStreamStore.Infrastructure;
    using SqlStreamStore.Logging;
    using SqlStreamStore.ScriptsV3;
    using SqlStreamStore.Streams;
    using SqlStreamStore.Subscriptions;

    /// <summary>
    ///     Represents a Microsoft SQL Server stream store implementation that
    ///     uses V3 of the store schema.
    /// </summary>
    public sealed partial class MsSqlStreamStoreV3 : StreamStoreBase
    {
        private readonly Func<SqlConnection> _createConnection;
        private readonly Lazy<IStreamStoreNotifier> _streamStoreNotifier;
        private readonly Scripts _scripts;
        private readonly SqlMetaData[] _appendToStreamSqlMetadata;
        private readonly MsSqlStreamStoreV3Settings _settings;
        private readonly int _commandTimeout;
        public const int FirstSchemaVersion = 1;
        public const int CurrentSchemaVersion = 3;

        /// <summary>
        ///     Initializes a new instance of <see cref="MsSqlStreamStoreV3"/>
        /// </summary>
        /// <param name="settings">A settings class to configure this instance.</param>
        public MsSqlStreamStoreV3(MsSqlStreamStoreV3Settings settings)
            :base(settings.GetUtcNow, settings.LogName)
        {
            Ensure.That(settings, nameof(settings)).IsNotNull();
            _settings = settings;

            _createConnection = () => settings.ConnectionFactory(settings.ConnectionString);
            _streamStoreNotifier = new Lazy<IStreamStoreNotifier>(() =>
                {
                    if(settings.CreateStreamStoreNotifier == null)
                    {
                        throw new InvalidOperationException(
                            "Cannot create notifier because supplied createStreamStoreNotifier was null");
                    }
                    return settings.CreateStreamStoreNotifier.Invoke(this);
                });
            _scripts = new Scripts(settings.Schema);

            var sqlMetaData = new List<SqlMetaData>
            {
                new SqlMetaData("StreamVersion", SqlDbType.Int, true, false, SortOrder.Unspecified, -1),
                new SqlMetaData("Id", SqlDbType.UniqueIdentifier),
                new SqlMetaData("Created", SqlDbType.DateTime, true, false, SortOrder.Unspecified, -1),
                new SqlMetaData("Type", SqlDbType.NVarChar, 128),
                new SqlMetaData("JsonData", SqlDbType.NVarChar, SqlMetaData.Max),
                new SqlMetaData("JsonMetadata", SqlDbType.NVarChar, SqlMetaData.Max)
            };

            if(settings.GetUtcNow != null)
            {
                // Created column value will be client supplied so should prevent using of the column default function
                sqlMetaData[2] = new SqlMetaData("Created", SqlDbType.DateTime);
            }

            _appendToStreamSqlMetadata = sqlMetaData.ToArray();
            _commandTimeout = settings.CommandTimeout;
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

            using(var connection = _createConnection())
            {
                await connection.OpenAsync(cancellationToken).NotOnCapturedContext();

                using (var command = new SqlCommand(_scripts.CreateSchema, connection))
                {
                    command.CommandTimeout = _commandTimeout;
                    await command
                        .ExecuteNonQueryAsync(cancellationToken)
                        .NotOnCapturedContext();
                }
            }
        }

        /// <summary>
        ///     Checks the store schema for the correct version.
        /// </summary>
        /// <param name="cancellationToken"></param>
        /// <returns>A <see cref="CheckSchemaResult"/> representing the result of the operation.</returns>
        public async Task<CheckSchemaResult> CheckSchema(
            CancellationToken cancellationToken = default)
        {
            GuardAgainstDisposed();

            using(var connection = _createConnection())
            {
                await connection.OpenAsync(cancellationToken).NotOnCapturedContext();

                using (var command = new SqlCommand(_scripts.GetSchemaVersion, connection))
                {
                    command.CommandTimeout = _commandTimeout;
                    var extendedProperties =  await command
                        .ExecuteReaderAsync(cancellationToken)
                        .NotOnCapturedContext();

                    int? version = null;
                    while(await extendedProperties.ReadAsync(cancellationToken))
                    {
                        if(extendedProperties.GetString(0) != "version")
                        {
                            continue;
                        }
                        version = int.Parse(extendedProperties.GetString(1));
                        break;
                    }

                    return version == null 
                        ? new CheckSchemaResult(FirstSchemaVersion, CurrentSchemaVersion)  // First schema (1) didn't have extended properties.
                        : new CheckSchemaResult(int.Parse(version.ToString()), CurrentSchemaVersion);
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

                using(var command = new SqlCommand(_scripts.DropAll, connection))
                {
                    command.CommandTimeout = _commandTimeout;
                    await command
                        .ExecuteNonQueryAsync(cancellationToken)
                        .NotOnCapturedContext();
                }
            }
        }

        private async Task<int> GetStreamMessageCount(
            string streamId,
            CancellationToken cancellationToken = default)
        {
            GuardAgainstDisposed();

            using(var connection = _createConnection())
            {
                await connection.OpenAsync(cancellationToken).NotOnCapturedContext();

                using(var command = new SqlCommand(_scripts.GetStreamMessageCount, connection))
                {
                    var streamIdInfo = new StreamIdInfo(streamId);
                    command.CommandTimeout = _commandTimeout;
                    command.Parameters.Add(new SqlParameter("streamId", SqlDbType.Char, 42) { Value = streamIdInfo.SqlStreamId.Id });

                    var result = await command
                        .ExecuteScalarAsync(cancellationToken)
                        .NotOnCapturedContext();

                    return (int) result;
                }
            }
        }

        ///  <summary>
        ///      Migrates a V2 schema to a V3 schema. The V3 schema maintains a
        ///      copy of 'MaxAge' and 'MaxCount' in the streams table for
        ///      performance reasons. This modifies the schema and iterates over
        ///      all streams lifting `MaxAge` and `MaxCount` if the stream has
        ///      metadata. Migration progress is logged and reported.
        /// 
        ///      As usual, ensure you have the database backed up.
        ///  </summary>
        /// <param name="progress">A provider that can receive progress updates.</param>
        /// <param name="cancellationToken">The cancellation instruction.</param>
        /// <returns>A Task representing the asynchronous operation.</returns>
        public async Task Migrate(IProgress<MigrateProgress> progress, CancellationToken cancellationToken)
        {
            GuardAgainstDisposed();

            Logger.Info("Starting migration from schema v2 to v3...");

            try
            {
                var checkSchemaResult = await CheckSchema(cancellationToken);
                if(checkSchemaResult.IsMatch())
                {
                    Logger.Info("Nothing to do, schema is already v3.");
                    return;
                }

                if(checkSchemaResult.CurrentVersion != 2)
                {
                    string message = $"Schema did not match expected version for migtation - 2. Actual version is {checkSchemaResult.CurrentVersion}";
                    throw new InvalidOperationException(message);
                }
                progress.Report(new MigrateProgress(MigrateProgress.MigrateStage.SchemaVersionChecked));

                // Migrate the schema
                using (var connection = _createConnection())
                {
                    await connection.OpenAsync(cancellationToken).NotOnCapturedContext();

                    using(var command = new SqlCommand(_scripts.Migration_v3, connection))
                    {
                        command.CommandTimeout = _commandTimeout;
                        await command
                            .ExecuteNonQueryAsync(cancellationToken)
                            .NotOnCapturedContext();
                    }
                }
                progress.Report(new MigrateProgress(MigrateProgress.MigrateStage.SchemaMigrated));

                // Load up the stream IDs that have metadata.
                Logger.Info("Schema migrated. Starting data migration. Loading stream Ids...");
                HashSet<string> streamIds = new HashSet<string>();
                HashSet<string> metadataStreamIds = new HashSet<string>();

                var listStreamsResult = await ListStreams(int.MaxValue, cancellationToken: cancellationToken);

                foreach(var streamId in listStreamsResult.StreamIds)
                {
                    if(streamId.StartsWith("$$"))
                    {
                        metadataStreamIds.Add(streamId.Substring(2));
                    }
                    else
                    {
                        streamIds.Add(streamId);
                    }
                }

                streamIds.IntersectWith(metadataStreamIds);
                progress.Report(new MigrateProgress(MigrateProgress.MigrateStage.StreamIdsLoaded));

                // Migrate data
                Logger.Info("{count} streams to be processed...", streamIds.Count);
                int i = 0;
                foreach (var streamId in streamIds)
                {
                    var metadata = await GetStreamMetadataInternal(streamId, cancellationToken);
                    if(metadata != null)
                    {
                        Logger.Info("Migrating stream {streamId} ({current}/{total}", streamId, i, streamIds.Count);

                        using (var connection = _createConnection())
                        {
                            await connection.OpenAsync(cancellationToken).NotOnCapturedContext();

                            using(var command = new SqlCommand(_scripts.SetStreamMetadata, connection))
                            {
                                command.CommandTimeout = _commandTimeout;
                                command.Parameters.Add(new SqlParameter("streamId", SqlDbType.Char, 42) { Value = new StreamIdInfo(streamId).SqlStreamId.Id });
                                command.Parameters.AddWithValue("streamIdOriginal", "ignored");
                                command.Parameters.Add("maxAge", SqlDbType.Int);
                                command.Parameters["maxAge"].Value = (object)metadata.MaxAge ?? DBNull.Value;
                                command.Parameters.Add("maxCount", SqlDbType.Int);
                                command.Parameters["maxCount"].Value = (object)metadata.MaxCount ?? DBNull.Value;
                                await command.ExecuteNonQueryAsync(cancellationToken);
                            }
                        }
                    }
                    i++;
                }
                progress.Report(new MigrateProgress(MigrateProgress.MigrateStage.MetadataMigrated));
            }
            catch(Exception ex)
            {
                Logger.ErrorException("Error occured during migration", ex);
                throw;
            }
        }

        protected override async Task<long> ReadHeadPositionInternal(CancellationToken cancellationToken)
        {
            GuardAgainstDisposed();

            using(var connection = _createConnection())
            {
                await connection.OpenAsync(cancellationToken).NotOnCapturedContext();

                using(var command = new SqlCommand(_scripts.ReadHeadPosition, connection))
                {
                    command.CommandTimeout = _commandTimeout;
                    var result = await command
                        .ExecuteScalarAsync(cancellationToken)
                        .NotOnCapturedContext();

                    if(result == DBNull.Value)
                    {
                        return Position.End;
                    }
                    return (long) result;
                }
            }
        }

        protected override async Task<long> ReadStreamHeadPositionInternal(string streamId, CancellationToken cancellationToken)
        {
            GuardAgainstDisposed();

            using(var connection = _createConnection())
            {
                await connection.OpenAsync(cancellationToken).NotOnCapturedContext();

                using(var command = new SqlCommand(_scripts.ReadStreamHeadPosition, connection))
                {
                    command.CommandTimeout = _commandTimeout;
                    command.Parameters.Add(new SqlParameter("streamId", SqlDbType.Char, 42) { Value = new StreamIdInfo(streamId).SqlStreamId.Id });
                    var result = await command
                        .ExecuteScalarAsync(cancellationToken)
                        .NotOnCapturedContext();

                    if(result == null)
                    {
                        return Position.End;
                    }
                    return (long) result;
                }
            }
        }

        protected override async Task<int> ReadStreamHeadVersionInternal(string streamId, CancellationToken cancellationToken)
        {
            GuardAgainstDisposed();

            using(var connection = _createConnection())
            {
                await connection.OpenAsync(cancellationToken).NotOnCapturedContext();

                using(var command = new SqlCommand(_scripts.ReadStreamHeadVersion, connection))
                {
                    command.CommandTimeout = _commandTimeout;
                    command.Parameters.Add(new SqlParameter("streamId", SqlDbType.Char, 42) { Value = new StreamIdInfo(streamId).SqlStreamId.Id });
                    var result = await command
                        .ExecuteScalarAsync(cancellationToken)
                        .NotOnCapturedContext();

                    if(result == null)
                    {
                        return StreamVersion.End;
                    }
                    return (int) result;
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

        private IObservable<Unit> GetStoreObservable => _streamStoreNotifier.Value;

        /// <summary>
        /// Returns the script that can be used to create the Sql Stream Store in a Postgres database.
        /// </summary>
        /// <returns>The database creation script.</returns>
        public string GetSchemaCreationScript()
        {
            return _scripts.CreateSchema;
        }
    }
}