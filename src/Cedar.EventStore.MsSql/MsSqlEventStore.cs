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
    using Cedar.EventStore.SqlScripts;
    using EnsureThat;
    using Microsoft.SqlServer.Server;

    public class MsSqlEventStore : IEventStore
    {
        private readonly SqlConnection _connection;
        private readonly InterlockedBoolean _isDisposed = new InterlockedBoolean();

        public MsSqlEventStore(Func<SqlConnection> createConnection)
        {
            Ensure.That(createConnection, "createConnection").IsNotNull();

            _connection = createConnection();
            _connection.Open();
        }

        public async Task AppendToStream(
            string streamId,
            int expectedVersion,
            IEnumerable<NewStreamEvent> events,
            CancellationToken cancellationToken = default(CancellationToken))
        {
            Ensure.That(streamId, "streamId").IsNotNullOrWhiteSpace();
            Ensure.That(expectedVersion, "expectedVersion").IsGte(-2);
            Ensure.That(events, "events").IsNotNull();

            var streamIdInfo = HashStreamId(streamId);

            if(expectedVersion == ExpectedVersion.NoStream)
            {
                var sqlMetadata = new[]
                {
                    new SqlMetaData("StreamRevision", SqlDbType.Int, true, false, SortOrder.Unspecified, -1),
                    new SqlMetaData("Id", SqlDbType.UniqueIdentifier),
                    new SqlMetaData("Created", SqlDbType.DateTime, true, false, SortOrder.Unspecified, -1),
                    new SqlMetaData("Type", SqlDbType.NVarChar, 128),
                    new SqlMetaData("JsonData", SqlDbType.NVarChar, SqlMetaData.Max),
                    new SqlMetaData("JsonMetadata", SqlDbType.NVarChar, SqlMetaData.Max),
                };

                var sqlDataRecords = events.Select(@event =>
                {
                    var record = new SqlDataRecord(sqlMetadata);
                    record.SetGuid(1, @event.EventId);
                    record.SetString(3, @event.Type);
                    record.SetString(4, @event.JsonData);
                    record.SetString(5, @event.JsonMetadata);
                    return record;
                }).ToArray();

                using(var command = new SqlCommand(Scripts.CreateStream, _connection))
                {
                    command.Parameters.AddWithValue("streamId", streamIdInfo.StreamId);
                    command.Parameters.AddWithValue("streamIdOriginal", streamIdInfo.StreamIdOriginal);
                    var eventsParam = new SqlParameter("events", SqlDbType.Structured)
                    {
                        TypeName = "dbo.NewStreamEvents",
                        Value = sqlDataRecords
                    };
                    command.Parameters.Add(eventsParam);
                    await command.ExecuteNonQueryAsync(cancellationToken);
                }
            }
        }

        public Task DeleteStream(
            string streamId,
            int expectedVersion = ExpectedVersion.Any,
            CancellationToken cancellationToken = default(CancellationToken))
        {
            Ensure.That(streamId, "streamId").IsNotNullOrWhiteSpace();
            Ensure.That(expectedVersion, "expectedVersion").IsGte(-2);

            var streamIdInfo = HashStreamId(streamId);

            return expectedVersion == ExpectedVersion.Any
                ? DeleteStreamAnyVersion(streamIdInfo.StreamId, cancellationToken)
                : DeleteStreamExpectedVersion(streamIdInfo.StreamId, expectedVersion, cancellationToken);
        }

        private async Task DeleteStreamAnyVersion(
            string streamId,
            CancellationToken cancellationToken)
        {
            using (var command = new SqlCommand(Scripts.DeleteStreamAnyVersion, _connection))
            {
                command.Parameters.AddWithValue("streamId", streamId);
                await command.ExecuteNonQueryAsync(cancellationToken);
            }
        }


        private Task DeleteStreamExpectedVersion(
            string streamId,
            int expectedVersion,
            CancellationToken cancellationToken)
        {
            throw new NotImplementedException();
        }

        public async Task<AllEventsPage> ReadAll(
            Checkpoint checkpoint,
            int maxCount,
            ReadDirection direction = ReadDirection.Forward,
            CancellationToken cancellationToken = default(CancellationToken))
        {
            Ensure.That(checkpoint, "checkpoint").IsNotNull();
            Ensure.That(maxCount, "maxCount").IsGt(0);

            if(_isDisposed.Value)
            {
                throw new ObjectDisposedException("MsSqlEventStore");
            }

            long ordinal = checkpoint.GetOrdinal();

            var commandText = direction == ReadDirection.Forward ? Scripts.ReadAllForward : Scripts.ReadAllBackward;

            using (var command = new SqlCommand(commandText, _connection))
            {
                command.Parameters.AddWithValue("ordinal", ordinal);
                command.Parameters.AddWithValue("count", maxCount + 1); //Read extra row to see if at end or not
                var reader = await command.ExecuteReaderAsync(cancellationToken);

                List<StreamEvent> streamEvents = new List<StreamEvent>();
                if(!reader.HasRows)
                {
                    return new AllEventsPage(checkpoint.Value,
                        null,
                        true,
                        direction,
                        streamEvents.ToArray());
                }
                while(await reader.ReadAsync(cancellationToken))
                {
                    var streamId = reader.GetString(0);
                    var streamRevision = reader.GetInt32(1);
                    ordinal = reader.GetInt64(2);
                    var eventId = reader.GetGuid(3);
                    var created = reader.GetDateTime(4);
                    var type = reader.GetString(5);
                    var jsonData = reader.GetString(6);
                    var jsonMetadata = reader.GetString(7);

                    var streamEvent = new StreamEvent(streamId,
                        eventId,
                        streamRevision,
                        ordinal.ToString(),
                        created,
                        type,
                        jsonData,
                        jsonMetadata);

                    streamEvents.Add(streamEvent);
                }

                bool isEnd = true;
                string nextCheckpoint = null;

                if(streamEvents.Count == maxCount + 1)
                {
                    isEnd = false;
                    nextCheckpoint = streamEvents[maxCount].Checkpoint;
                    streamEvents.RemoveAt(maxCount);
                }

                return new AllEventsPage(checkpoint.Value,
                    nextCheckpoint,
                    isEnd,
                    direction,
                    streamEvents.ToArray());
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

            var streamIdInfo = HashStreamId(streamId);

            var streamRevision = start == StreamPosition.End ? int.MaxValue : start;
            string commandText;
            Func<List<StreamEvent>, int> getNextSequenceNumber;
            if(direction == ReadDirection.Forward)
            {
                commandText = Scripts.ReadStreamForward;
                getNextSequenceNumber = events => events.Last().StreamRevision + 1;
            }
            else
            {
                commandText = Scripts.ReadStreamBackward;
                getNextSequenceNumber = events => events.Last().StreamRevision - 1;
            }

            using (var command = new SqlCommand(commandText, _connection))
            {
                command.Parameters.AddWithValue("streamId", streamIdInfo.StreamId);
                command.Parameters.AddWithValue("count", count + 1); //Read extra row to see if at end or not
                command.Parameters.AddWithValue("streamRevision", streamRevision);

                List<StreamEvent> streamEvents = new List<StreamEvent>();

                var reader = await command.ExecuteReaderAsync(cancellationToken);
                await reader.ReadAsync(cancellationToken);
                bool doesNotExist = reader.IsDBNull(0);
                if (doesNotExist)
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
                await reader.NextResultAsync(cancellationToken);
                while (await reader.ReadAsync(cancellationToken))
                {
                    var streamRevision1 = reader.GetInt32(0);
                    var ordinal = reader.GetInt64(1);
                    var eventId = reader.GetGuid(2);
                    var created = reader.GetDateTime(3);
                    var type = reader.GetString(4);
                    var jsonData = reader.GetString(5);
                    var jsonMetadata = reader.GetString(6);

                    var streamEvent = new StreamEvent(streamId,
                        eventId,
                        streamRevision1,
                        ordinal.ToString(),
                        created,
                        type,
                        jsonData,
                        jsonMetadata);

                    streamEvents.Add(streamEvent);
                }

                // Read last event revision result set
                await reader.NextResultAsync(cancellationToken);
                await reader.ReadAsync(cancellationToken);
                var lastStreamRevision = reader.GetInt32(0);


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
                    lastStreamRevision,
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
            _connection.Dispose();
        }

        public async Task InitializeStore(
            bool ignoreErrors = false,
            CancellationToken cancellationToken = default(CancellationToken))
        {
            var cmd = new SqlCommand(Scripts.InitializeStore, _connection);
            if(ignoreErrors)
            {
                await ExecuteAndIgnoreErrors(() => cmd.ExecuteNonQueryAsync(cancellationToken));
            }
            else
            {
                await cmd.ExecuteNonQueryAsync(cancellationToken);
            }
        }

        public async Task DropAll(
            bool ignoreErrors = false,
            CancellationToken cancellationToken = default(CancellationToken))
        {
            var cmd = new SqlCommand(Scripts.DropAll, _connection);
            if(ignoreErrors)
            {
                await ExecuteAndIgnoreErrors(() => cmd.ExecuteNonQueryAsync(cancellationToken));
            }
            else
            {
                await cmd.ExecuteNonQueryAsync(cancellationToken);
            }
        }

        private static async Task<T> ExecuteAndIgnoreErrors<T>(Func<Task<T>> operation)
        {
            try
            {
                return await operation();
            }
            catch
            {
                return default(T);
            }
        }

        private static StreamIdInfo HashStreamId(string streamId)
        {
            Ensure.That(streamId, "streamId").IsNotNullOrWhiteSpace();

            Guid _;
            if(Guid.TryParse(streamId, out _))
            {
                return new StreamIdInfo(streamId, streamId);
            }

            byte[] hashBytes = SHA1.Create().ComputeHash(Encoding.UTF8.GetBytes(streamId));
            var hashedStreamId = BitConverter.ToString(hashBytes).Replace("-", "");
            return new StreamIdInfo(hashedStreamId, streamId);
        }

        private class StreamIdInfo
        {
            public readonly string StreamId;
            public readonly string StreamIdOriginal;

            public StreamIdInfo(string streamId, string streamIdOriginal)
            {
                StreamId = streamId;
                StreamIdOriginal = streamIdOriginal;
            }
        }
    }
}