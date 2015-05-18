namespace Cedar.EventStore
{
    using System;
    using System.Collections.Generic;
    using System.Collections.ObjectModel;
    using System.Data;
    using System.Data.SqlClient;
    using System.Linq;
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
                    command.Parameters.AddWithValue("streamId", streamId);
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
            throw new NotImplementedException();
        }

        public async Task<AllEventsPage> ReadAll(
            string checkpoint,
            int maxCount,
            ReadDirection direction = ReadDirection.Forward,
            CancellationToken cancellationToken = default(CancellationToken))
        {
            if(_isDisposed.Value)
            {
                throw new ObjectDisposedException("MsSqlEventStore");
            }
            int ordinal = 0;
            if(!string.IsNullOrWhiteSpace(checkpoint) && !int.TryParse(checkpoint, out ordinal))
            {
                throw new InvalidOperationException("Cannot parse checkpoint");
            }

            using(var command = new SqlCommand(Scripts.ReadAllForward, _connection))
            {
                command.Parameters.AddWithValue("ordinal", ordinal);
                command.Parameters.AddWithValue("count", maxCount + 1); //Read extra row to see if at end or not
                var reader = await command.ExecuteReaderAsync(cancellationToken);

                List<StreamEvent> streamEvents = new List<StreamEvent>();
                if(!reader.HasRows)
                {
                    return new AllEventsPage(checkpoint,
                        null,
                        true,
                        direction,
                        new ReadOnlyCollection<StreamEvent>(streamEvents));
                }
                while(await reader.ReadAsync(cancellationToken))
                {
                    var streamId = reader.GetString(0);
                    var streamRevision = reader.GetInt32(1);
                    ordinal = reader.GetInt32(2);
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

                return new AllEventsPage(checkpoint,
                    nextCheckpoint,
                    isEnd,
                    direction,
                    new ReadOnlyCollection<StreamEvent>(streamEvents));
            }
        }

        public Task<StreamEventsPage> ReadStream(
            string streamId,
            int start,
            int count,
            ReadDirection direction = ReadDirection.Forward,
            CancellationToken cancellationToken = default(CancellationToken))
        {
            throw new NotImplementedException();
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
    }
}