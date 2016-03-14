namespace Cedar.EventStore
{
    using System;
    using System.Data.SqlClient;
    using System.Security.Cryptography;
    using System.Text;
    using System.Threading;
    using System.Threading.Tasks;
    using Cedar.EventStore.Infrastructure;
    using Cedar.EventStore.SqlScripts;
    using Cedar.EventStore.Streams;
    using Cedar.EventStore.Subscriptions;
    using EnsureThat;

    public sealed partial class MsSqlEventStore : EventStoreBase
    {
        private readonly Func<SqlConnection> _createConnection;
        private readonly AsyncLazy<IEventStoreNotifier> _eventStoreNotifier;
        private readonly Scripts _scripts;

        public MsSqlEventStore(
            string connectionString,
            CreateEventStoreNotifier createEventStoreNotifier,
            string schema = "dbo")
        {
            Ensure.That(connectionString, nameof(connectionString)).IsNotNullOrWhiteSpace();

            _createConnection = () => new SqlConnection(connectionString);
            _eventStoreNotifier = new AsyncLazy<IEventStoreNotifier>(
                async () => await createEventStoreNotifier(this), false);
            _scripts = new Scripts(schema);
        }

        protected override Task DeleteStreamInternal(
            string streamId,
            int expectedVersion,
            CancellationToken cancellationToken)
        {
            var streamIdInfo = new StreamIdInfo(streamId);

            return expectedVersion == ExpectedVersion.Any
                ? DeleteStreamAnyVersion(streamIdInfo, cancellationToken)
                : DeleteStreamExpectedVersion(streamIdInfo, expectedVersion, cancellationToken);
        }

        private async Task DeleteStreamAnyVersion(
            StreamIdInfo streamIdInfo,
            CancellationToken cancellationToken)
        {
            using(var connection = _createConnection())
            {
                await connection.OpenAsync(cancellationToken);

                using(var command = new SqlCommand(_scripts.DeleteStreamAnyVersion, connection))
                {
                    command.Parameters.AddWithValue("streamId", streamIdInfo.Hash);
                    await command
                        .ExecuteNonQueryAsync(cancellationToken)
                        .NotOnCapturedContext();
                }
            }
        }

        private async Task DeleteStreamExpectedVersion(
            StreamIdInfo streamIdInfo,
            int expectedVersion,
            CancellationToken cancellationToken)
        {
            using(var connection = _createConnection())
            {
                await connection.OpenAsync(cancellationToken).NotOnCapturedContext();

                using(var command = new SqlCommand(_scripts.DeleteStreamExpectedVersion, connection))
                {
                    command.Parameters.AddWithValue("streamId", streamIdInfo.Hash);
                    command.Parameters.AddWithValue("expectedStreamVersion", expectedVersion);
                    try
                    {
                        await command
                            .ExecuteNonQueryAsync(cancellationToken)
                            .NotOnCapturedContext();
                    }
                    catch(SqlException ex)
                    {
                        if(ex.Message == "WrongExpectedVersion")
                        {
                            throw new WrongExpectedVersionException(
                                Messages.DeleteStreamFailedWrongExpectedVersion(streamIdInfo.Id, expectedVersion),
                                ex);
                        }
                        throw;
                    }
                }
            }
        }

        public async Task InitializeStore(
            bool ignoreErrors = false,
            CancellationToken cancellationToken = default(CancellationToken))
        {
            CheckIfDisposed();

            using(var connection = _createConnection())
            {
                await connection.OpenAsync(cancellationToken).NotOnCapturedContext();

                if(_scripts.Schema != "dbo")
                {
                    using(var command = new SqlCommand($"CREATE SCHEMA {_scripts.Schema}", connection))
                    {
                        await command
                            .ExecuteNonQueryAsync(cancellationToken)
                            .NotOnCapturedContext();
                    }
                }

                using (var command = new SqlCommand(_scripts.InitializeStore, connection))
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
            CheckIfDisposed();

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
                        await command.ExecuteNonQueryAsync(cancellationToken)
                            .NotOnCapturedContext();
                    }
                }
            }
        }

        protected override async Task<long> ReadHeadCheckpointInternal(CancellationToken cancellationToken)
        {
            CheckIfDisposed();

            using(var connection = _createConnection())
            {
                await connection.OpenAsync(cancellationToken);

                using(var command = new SqlCommand(_scripts.ReadHeadCheckpoint, connection))
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
                if(_eventStoreNotifier.IsValueCreated)
                {
                    _eventStoreNotifier.Value.Dispose();
                }
            }
            base.Dispose(disposing);
        }

        private IObservable<Unit> GetStoreObservable => _eventStoreNotifier.Value.Result;

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

        private struct StreamIdInfo
        {
            public readonly string Hash;
            public readonly string Id;

            public StreamIdInfo(string id)
            {
                Ensure.That(id, "streamId").IsNotNullOrWhiteSpace();

                Id = id;

                Guid _;
                if(Guid.TryParse(id, out _))
                {
                    Hash = id;

                    return;
                }
                using(var sha1 = SHA1.Create())
                {
                    var hashBytes = sha1.ComputeHash(Encoding.UTF8.GetBytes(id));
                    Hash = BitConverter.ToString(hashBytes).Replace("-", string.Empty);
                }
            }
        }
    }
}