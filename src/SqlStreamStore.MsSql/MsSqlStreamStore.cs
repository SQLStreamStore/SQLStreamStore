namespace SqlStreamStore
{
    using System;
    using System.Collections.Generic;
    using System.Data;
    using System.Data.SqlClient;
    using System.Threading;
    using System.Threading.Tasks;
    using EnsureThat;
    using Microsoft.SqlServer.Server;
    using SqlStreamStore.Infrastructure;
    using SqlStreamStore.MsSqlScripts;
    using SqlStreamStore.Subscriptions;

    public sealed partial class MsSqlStreamStore : StreamStoreBase
    {
        private readonly Func<SqlConnection> _createConnection;
        private readonly Lazy<IStreamStoreNotifier> _streamStoreNotifier;
        private readonly Scripts _scripts;
        private readonly SqlMetaData[] _appendToStreamSqlMetadata;

        public MsSqlStreamStore(MsSqlStreamStoreSettings settings)
            :base(settings.MetadataMaxAgeCacheExpire, settings.MetadataMaxAgeCacheMaxSize,
                 settings.GetUtcNow, settings.LogName)
        {
            Ensure.That(settings, nameof(settings)).IsNotNull();

            _createConnection = () => new SqlConnection(settings.ConnectionString);
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

        public async Task CreateSchema(
            bool ignoreErrors = false,
            CancellationToken cancellationToken = default(CancellationToken))
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
                    if(ignoreErrors)
                    {
                        await ExecuteAndIgnoreErrors(() => command.ExecuteNonQueryAsync(cancellationToken))
                            .NotOnCapturedContext();
                    }
                    else
                    {
                        await command.ExecuteNonQueryAsync(cancellationToken)
                            .NotOnCapturedContext();
                    }
                }
            }
        }

        public async Task DropAll(
            bool ignoreErrors = false,
            CancellationToken cancellationToken = default(CancellationToken))
        {
            GuardAgainstDisposed();

            using(var connection = _createConnection())
            {
                await connection.OpenAsync(cancellationToken).NotOnCapturedContext();

                using(var command = new SqlCommand(_scripts.DropAll, connection))
                {
                    if(ignoreErrors)
                    {
                        await ExecuteAndIgnoreErrors(() => command.ExecuteNonQueryAsync(cancellationToken))
                            .NotOnCapturedContext();
                    }
                    else
                    {
                        await command
                            .ExecuteNonQueryAsync(cancellationToken)
                            .NotOnCapturedContext();
                    }
                }
            }
        }

        public override async Task<int> GetmessageCount(
            string streamId,
            CancellationToken cancellationToken = default(CancellationToken))
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

        public async Task<int> GetmessageCount(
            string streamId,
            DateTime createdBefore,
            CancellationToken cancellationToken = default(CancellationToken))
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

        protected override async Task<long> ReadHeadPositionInternal(CancellationToken cancellationToken)
        {
            GuardAgainstDisposed();

            using(var connection = _createConnection())
            {
                await connection.OpenAsync(cancellationToken);

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

        private static async Task<T> ExecuteAndIgnoreErrors<T>(Func<Task<T>> operation)
        {
            try
            {
                return await operation().NotOnCapturedContext();
            }
            catch
            {
                return default(T);
            }
        }
    }
}