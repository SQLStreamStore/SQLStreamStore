﻿namespace SqlStreamStore
{
    using System;
    using System.Collections.Generic;
    using System.Data;
    using System.Data.SqlClient;
    using System.Threading;
    using System.Threading.Tasks;
    using Microsoft.SqlServer.Server;
    using SqlStreamStore.Imports.Ensure.That;
    using SqlStreamStore.Infrastructure;
    using SqlStreamStore.Logging;
    using SqlStreamStore.ScriptsV3;
    using SqlStreamStore.Subscriptions;

    /// <summary>
    ///     Represents a Micrsoft SQL Server stream store implementation that
    ///     uses V3 of the store schema.
    /// </summary>
    public sealed partial class MsSqlStreamStoreV3 : StreamStoreBase
    {
        private readonly Func<SqlConnection> _createConnection;
        private readonly Lazy<IStreamStoreNotifier> _streamStoreNotifier;
        private readonly Scripts _scripts;
        private readonly SqlMetaData[] _appendToStreamSqlMetadata;
        private readonly MsSqlStreamStoreV3Settings _settings;
        public const int FirstSchemaVersion = 1;
        public const int CurrentSchemaVersion = 3;

        /// <summary>
        ///     Initializes a new instance of <see cref="MsSqlStreamStoreV3"/>
        /// </summary>
        /// <param name="settings">A settings class to configur this instance.</param>
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
        }

        /// <summary>
        ///     Creates a scheme to hold stream 
        /// </summary>
        /// <param name="cancellationToken">The cancellation instruction.</param>
        /// <returns>A <see cref="Task"/> representing the asynchronous operation.</returns>
        public async Task CreateSchema(CancellationToken cancellationToken = default)
        {
            GuardAgainstDisposed();

            using(var connection = _createConnection())
            {
                await connection.OpenAsync(cancellationToken).NotOnCapturedContext();

                if(_scripts.Schema != "dbo")
                {
                    using(var command = new SqlCommand($@"
                        IF NOT EXISTS (
                        SELECT  schema_name
                        FROM    information_schema.schemata
                        WHERE   schema_name = '{_scripts.Schema}' ) 

                        BEGIN
                        EXEC sp_executesql N'CREATE SCHEMA {_scripts.Schema}'
                        END", connection))
                    {
                        await command
                            .ExecuteNonQueryAsync(cancellationToken)
                            .NotOnCapturedContext();
                    }
                }

                using (var command = new SqlCommand(_scripts.CreateSchema, connection))
                {
                    await command.ExecuteNonQueryAsync(cancellationToken)
                        .NotOnCapturedContext();
                }
            }
        }

        /// <summary>
        /// 
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
                    var extendedProperties =  await command
                        .ExecuteReaderAsync(cancellationToken)
                        .NotOnCapturedContext();

                    int? version = null;
                    while(await extendedProperties.ReadAsync(cancellationToken))
                    {
                        if(extendedProperties.GetString(0) != "version")
                            continue;
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
                    await command
                        .ExecuteNonQueryAsync(cancellationToken)
                        .NotOnCapturedContext();
                }
            }
        }

        /// <inheritdoc />
        protected override async Task<int> GetStreamMessageCount(
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
                    command.Parameters.AddWithValue("streamId", streamIdInfo.SqlStreamId.Id);

                    var result = await command
                        .ExecuteScalarAsync(cancellationToken)
                        .NotOnCapturedContext();

                    return (int) result;
                }
            }
        }

        public async Task<int> GetMessageCount(
            string streamId,
            DateTime createdBefore,
            CancellationToken cancellationToken = default)
        {
            GuardAgainstDisposed();

            using (var connection = _createConnection())
            {
                await connection.OpenAsync(cancellationToken).NotOnCapturedContext();

                using (var command = new SqlCommand(_scripts.GetStreamMessageBeforeCreatedCount, connection))
                {
                    var streamIdInfo = new StreamIdInfo(streamId);
                    command.Parameters.AddWithValue("streamId", streamIdInfo.SqlStreamId.Id);
                    command.Parameters.AddWithValue("created", createdBefore);

                    var result = await command
                        .ExecuteScalarAsync(cancellationToken)
                        .NotOnCapturedContext();

                    return (int)result;
                }
            }
        }

        /// <summary>
        ///     Migrates a V2 schema to a V3 schema. The V3 schema maintains a
        ///     copy of 'MaxAge' and 'MaxCount' in the streams table for
        ///     performance reasons. This modifies the schema and iterates over
        ///     all streams lifting `MaxAge` and `MaxCount` if the stream has
        ///     metadata. Migration progress is logged.
        /// </summary>
        /// <param name="cancellationToken"></param>
        /// <returns></returns>
        public async Task Migrate(CancellationToken cancellationToken)
        {
            GuardAgainstDisposed();

            Logger.Info("Starting migration from schema v2 to v3...");

            try
            {
                // Migrate the schema
                using(var connection = _createConnection())
                {
                    await connection.OpenAsync(cancellationToken).NotOnCapturedContext();

                    using(var command = new SqlCommand(_scripts.Migration_v3, connection))
                    {
                        await command
                            .ExecuteNonQueryAsync(cancellationToken)
                            .NotOnCapturedContext();
                    }
                }

                // Loadup the stream IDs that have metadata.
                Logger.Info("Schema migrated. Starting data migration. Loading stream Ids...");
                HashSet<string> streamIds = new HashSet<string>();
                HashSet<string> metadataStreamIds = new HashSet<string>();
                using(var connection = _createConnection())
                {
                    await connection.OpenAsync(cancellationToken).NotOnCapturedContext();

                    using(var command = new SqlCommand(_scripts.ListStreamIds, connection))
                    {
                        var reader = await command
                            .ExecuteReaderAsync(cancellationToken)
                            .NotOnCapturedContext();

                        while(await reader.ReadAsync(cancellationToken))
                        {
                            var streamId = reader.GetString(0);
                            if(streamId.StartsWith("$$"))
                            {
                                metadataStreamIds.Add(streamId.Substring(2));
                            }
                            else
                            {
                                streamIds.Add(streamId);
                            }
                        }
                    }
                }
                streamIds.IntersectWith(metadataStreamIds);

                // Migrate data
                Logger.Info($"{streamIds.Count} streams to be processed...");
                int i = 0;
                foreach (var streamId in streamIds)
                {
                    var metadata = await GetStreamMetadataInternal(streamId, cancellationToken);
                    if(metadata != null)
                    {
                        Logger.Info($"Migrating stream {streamId} ({i}/{streamIds.Count}");

                        using (var connection = _createConnection())
                        {
                            await connection.OpenAsync(cancellationToken).NotOnCapturedContext();

                            using(var command = new SqlCommand(_scripts.SetStreamMetadata, connection))
                            {
                                command.Parameters.AddWithValue("streamId", new StreamIdInfo(streamId).SqlStreamId.Id);
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