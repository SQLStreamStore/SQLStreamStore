namespace Cedar.EventStore
{
    using System;
    using System.Collections.Generic;
    using System.Data;
    using System.Data.SqlClient;
    using System.Linq;
    using System.Security.Cryptography;
    using System.Text;
    using System.Threading;
    using System.Threading.Tasks;
    using Cedar.EventStore.Infrastructure;
    using Cedar.EventStore.SqlScripts;
    using Cedar.EventStore.Streams;
    using Cedar.EventStore.Subscriptions;
    using EnsureThat;
    using Microsoft.SqlServer.Server;

    public sealed partial class MsSqlEventStore : IEventStore
    {
        private readonly Func<SqlConnection> _createConnection;
        private readonly InterlockedBoolean _isDisposed = new InterlockedBoolean();
        private readonly SqlMetaData[] _appendToStreamSqlMetadata =
        {
            new SqlMetaData("StreamVersion", SqlDbType.Int, true, false, SortOrder.Unspecified, -1),
            new SqlMetaData("Id", SqlDbType.UniqueIdentifier),
            new SqlMetaData("Created", SqlDbType.DateTime, true, false, SortOrder.Unspecified, -1),
            new SqlMetaData("Type", SqlDbType.NVarChar, 128),
            new SqlMetaData("JsonData", SqlDbType.NVarChar, SqlMetaData.Max),
            new SqlMetaData("JsonMetadata", SqlDbType.NVarChar, SqlMetaData.Max),
        };

        private readonly Lazy<Task<SqlEventsWatcher>> _lazySqlEventsWatcher;

        public MsSqlEventStore(string connectionString)
        {
            Ensure.That(connectionString, nameof(connectionString)).IsNotNullOrWhiteSpace();

            _createConnection = () => new SqlConnection(connectionString);
            _lazySqlEventsWatcher = new Lazy<Task<SqlEventsWatcher>>(() => CreateSqlEventsWatcher(connectionString));
        }

        public string StartCheckpoint => LongCheckpoint.Start.Value;

        public string EndCheckpoint => LongCheckpoint.End.Value;

        public Task DeleteStream(
            string streamId,
            int expectedVersion = ExpectedVersion.Any,
            CancellationToken cancellationToken = default(CancellationToken))
        {
            Ensure.That(streamId, "streamId").IsNotNullOrWhiteSpace();
            Ensure.That(expectedVersion, "expectedVersion").IsGte(-2);
            CheckIfDisposed();

            var streamIdInfo = new StreamIdInfo(streamId);

            return expectedVersion == ExpectedVersion.Any
                ? DeleteStreamAnyVersion(streamIdInfo, cancellationToken)
                : DeleteStreamExpectedVersion(streamIdInfo, expectedVersion, cancellationToken);
        }

        private async Task DeleteStreamAnyVersion(
            StreamIdInfo streamIdInfo,
            CancellationToken cancellationToken)
        {
            CheckIfDisposed();

            using (var connection = _createConnection())
            {
                await connection.OpenAsync(cancellationToken);

                using(var command = new SqlCommand(Scripts.DeleteStreamAnyVersion, connection))
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
            using (var connection = _createConnection())
            {
                await connection.OpenAsync(cancellationToken).NotOnCapturedContext();

                using(var command = new SqlCommand(Scripts.DeleteStreamExpectedVersion, connection))
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
                                string.Format(Messages.DeleteStreamFailedWrongExpectedVersion, streamIdInfo.Id, expectedVersion),
                                ex);
                        }
                        throw;
                    }
                }
            }
        }

        public async Task<StreamEventsPage> ReadStream(
            string streamId,
            int start,
            int count,
            ReadDirection direction = ReadDirection.Forward,
            CancellationToken cancellationToken = default(CancellationToken))
        {
            Ensure.That(streamId, "streamId").IsNotNull();
            Ensure.That(start, "start").IsGte(-1);
            Ensure.That(count, "count").IsGte(0);
            CheckIfDisposed();

            using (var connection = _createConnection())
            {
                await connection.OpenAsync(cancellationToken).NotOnCapturedContext();

                return await ReadStreamInternal(streamId, start, count, direction, connection, cancellationToken);
            }
        }

        public async Task InitializeStore(
            bool ignoreErrors = false,
            CancellationToken cancellationToken = default(CancellationToken))
        {
            CheckIfDisposed();

            using (var connection = _createConnection())
            {
                await connection.OpenAsync(cancellationToken).NotOnCapturedContext();

                using(var command = new SqlCommand(Scripts.InitializeStore, connection))
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

            using (var connection = _createConnection())
            {
                await connection.OpenAsync(cancellationToken).NotOnCapturedContext();

                using(var command = new SqlCommand(Scripts.DropAll, connection))
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

        private static async Task<StreamEventsPage> ReadStreamInternal(
            string streamId,
            int start,
            int count,
            ReadDirection direction,
            SqlConnection connection,
            CancellationToken cancellationToken)
        {
            var streamIdInfo = new StreamIdInfo(streamId);

            var streamVersion = start == StreamPosition.End ? int.MaxValue : start; // To read backwards from end, need to use int MaxValue
            string commandText;
            Func<List<StreamEvent>, int> getNextSequenceNumber;
            if(direction == ReadDirection.Forward)
            {
                commandText = Scripts.ReadStreamForward;
                getNextSequenceNumber = events => events.Last().StreamVersion + 1;
            }
            else
            {
                commandText = Scripts.ReadStreamBackward;
                getNextSequenceNumber = events => events.Last().StreamVersion - 1;
            }

            using(var command = new SqlCommand(commandText, connection))
            {
                command.Parameters.AddWithValue("streamId", streamIdInfo.Hash);
                command.Parameters.AddWithValue("count", count + 1); //Read extra row to see if at end or not
                command.Parameters.AddWithValue("StreamVersion", streamVersion);

                List<StreamEvent> streamEvents = new List<StreamEvent>();

                var reader = await command.ExecuteReaderAsync(cancellationToken).NotOnCapturedContext();
                await reader.ReadAsync(cancellationToken).NotOnCapturedContext();
                bool doesNotExist = reader.IsDBNull(0);
                if(doesNotExist)
                {
                    return new StreamEventsPage(streamId,
                        PageReadStatus.StreamNotFound,
                        start,
                        -1,
                        -1,
                        direction,
                        isEndOfStream: true);
                }

                // Read IsDeleted result set
                var isDeleted = reader.GetBoolean(0);
                if(isDeleted)
                {
                    return new StreamEventsPage(streamId,
                        PageReadStatus.StreamDeleted,
                        0,
                        0,
                        0,
                        direction,
                        isEndOfStream: true);
                }


                // Read Events result set
                await reader.NextResultAsync(cancellationToken).NotOnCapturedContext();
                while(await reader.ReadAsync(cancellationToken).NotOnCapturedContext())
                {
                    var streamVersion1 = reader.GetInt32(0);
                    var ordinal = reader.GetInt64(1);
                    var eventId = reader.GetGuid(2);
                    var created = reader.GetDateTime(3);
                    var type = reader.GetString(4);
                    var jsonData = reader.GetString(5);
                    var jsonMetadata = reader.GetString(6);

                    var streamEvent = new StreamEvent(streamId,
                        eventId,
                        streamVersion1,
                        ordinal.ToString(),
                        created,
                        type,
                        jsonData,
                        jsonMetadata);

                    streamEvents.Add(streamEvent);
                }

                // Read last event revision result set
                await reader.NextResultAsync(cancellationToken).NotOnCapturedContext();
                await reader.ReadAsync(cancellationToken).NotOnCapturedContext();
                var lastStreamVersion = reader.GetInt32(0);

                bool isEnd = true;
                if(streamEvents.Count == count + 1)
                {
                    isEnd = false;
                    streamEvents.RemoveAt(count);
                }

                return new StreamEventsPage(
                    streamId,
                    PageReadStatus.Success,
                    start,
                    getNextSequenceNumber(streamEvents),
                    lastStreamVersion,
                    direction,
                    isEnd,
                    streamEvents.ToArray());
            }
        }

        public void Dispose()
        {
            if(_isDisposed.EnsureCalledOnce())
            {
                return;
            }
            if(_lazySqlEventsWatcher.IsValueCreated)
            {
                _lazySqlEventsWatcher.Value.Dispose();
            }
        }

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

        private async Task<SqlEventsWatcher> CreateSqlEventsWatcher(string connectionString)
        {
            var watcher = new SqlEventsWatcher(connectionString);
            await watcher.Initialize();
            return watcher;
        }

        private void CheckIfDisposed()
        {
            if(_isDisposed.Value)
            {
                throw new ObjectDisposedException(nameof(MsSqlEventStore));
            }
        }

        private struct StreamIdInfo
        {
            private static readonly SHA1 s_sha1 = SHA1.Create();
            public readonly string Hash;
            public readonly string Id;

            public StreamIdInfo(string id)
            {
                Ensure.That(id, "streamId").IsNotNullOrWhiteSpace();

                Id = id;

                Guid _;
                if (Guid.TryParse(id, out _))
                {
                    Hash = id;
                }

                byte[] hashBytes = s_sha1.ComputeHash(Encoding.UTF8.GetBytes(id));
                Hash = BitConverter.ToString(hashBytes).Replace("-", string.Empty);
            }
        }
    }
}