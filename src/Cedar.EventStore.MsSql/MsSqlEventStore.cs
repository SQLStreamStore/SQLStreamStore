namespace Cedar.EventStore
{
    using System;
    using System.Collections.Generic;
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
                    new SqlMetaData("Id", SqlDbType.UniqueIdentifier, true, false, SortOrder.Unspecified, -1),
                    new SqlMetaData("Created", SqlDbType.DateTime, true, false, SortOrder.Unspecified, -1),
                    new SqlMetaData("Type", SqlDbType.NVarChar, 128),
                    new SqlMetaData("JsonData", SqlDbType.NVarChar, SqlMetaData.Max),
                    new SqlMetaData("JsonMetadata", SqlDbType.NVarChar, SqlMetaData.Max),
                };

                var sqlDataRecords = events.Select(@event =>
                {
                    var record = new SqlDataRecord(sqlMetadata);
                    record.SetString(3, @event.Type);
                    record.SetString(4, @event.Type);
                    record.SetString(5, @event.Type);
                    return record;
                }).ToArray();

                using(var command = new SqlCommand(Scripts.CreateStream, _connection))
                {
                    command.Parameters.AddWithValue("streamId", streamId);
                    var eventsParam = new SqlParameter("@events", SqlDbType.Structured)
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

        public Task<AllEventsPage> ReadAll(
            string checkpoint,
            int maxCount,
            ReadDirection direction = ReadDirection.Forward,
            CancellationToken cancellationToken = default(CancellationToken))
        {
            throw new NotImplementedException();
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