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
    using Cedar.EventStore.Exceptions;
    using Cedar.EventStore.SqlScripts;
    using EnsureThat;
    using Microsoft.SqlServer.Server;

    public sealed class MsSqlEventStore : IEventStore
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

        public MsSqlEventStore(Func<SqlConnection> createConnection)
        {
            Ensure.That(createConnection, "createConnection").IsNotNull();

            _createConnection = createConnection;
        }

        public Task AppendToStream(
            string streamId,
            int expectedVersion,
            NewStreamEvent[] events,
            CancellationToken cancellationToken = default(CancellationToken))
        {
            Ensure.That(streamId, "streamId").IsNotNullOrWhiteSpace();
            Ensure.That(expectedVersion, "expectedVersion").IsGte(-2);
            Ensure.That(events, "events").IsNotNull();
            CheckIfDisposed();

            var streamIdHash = HashStreamId(streamId);

            switch(expectedVersion)
            {
                case ExpectedVersion.Any:
                    return AppendToStreamExpectedVersionAny(streamId, expectedVersion, events, streamIdHash, cancellationToken);
                case ExpectedVersion.NoStream:
                    return AppendToStreamExpectedVersionNoStream(streamId, expectedVersion, events, streamIdHash, cancellationToken);
                default:
                    return AppendToStreamExpectedVersion(streamId, expectedVersion, events, streamIdHash, cancellationToken);
            }
        }

        private async Task AppendToStreamExpectedVersionAny(
            string streamId,
            int expectedVersion,
            NewStreamEvent[] events,
            StreamIdInfo streamIdHash,
            CancellationToken cancellationToken)
        {
            var sqlDataRecords = CreateSqlDataRecords(events);

            using (var connection = _createConnection())
            {
                await connection.OpenAsync(cancellationToken).NotOnCapturedContext();

                using (var command = new SqlCommand(Scripts.AppendStreamExpectedVersionAny, connection))
                {
                    command.Parameters.AddWithValue("streamId", streamIdHash.StreamId);
                    command.Parameters.AddWithValue("streamIdOriginal", streamIdHash.StreamIdOriginal);
                    var eventsParam = CreateNewEventsSqlParameter(sqlDataRecords);
                    command.Parameters.Add(eventsParam);

                    try
                    {
                        await command
                            .ExecuteNonQueryAsync(cancellationToken)
                            .NotOnCapturedContext();
                    }
                    catch (SqlException ex)
                    {
                        // Check for unique constraint violation on 
                        // https://technet.microsoft.com/en-us/library/aa258747%28v=sql.80%29.aspx
                        if (ex.IsUniqueConstraintViolationOnIndex("IX_Events_StreamIdInternal_Id"))
                        {
                            // Idempotency handling. Check if the events have already been written.

                            var page = await ReadStreamInternal(
                                    streamId,
                                    StreamPosition.Start,
                                    events.Length,
                                    ReadDirection.Forward,
                                    connection,
                                    cancellationToken)
                                    .NotOnCapturedContext();

                            if (events.Length > page.Events.Count)
                            {
                                throw new WrongExpectedVersionException(
                                    Messages.AppendFailedWrongExpectedVersion.FormatWith(streamId, expectedVersion),
                                    ex);
                            }

                            for (int i = 0; i < Math.Min(events.Length, page.Events.Count); i++)
                            {
                                if (events[i].EventId != page.Events[i].EventId)
                                {
                                    throw new WrongExpectedVersionException(
                                        Messages.AppendFailedWrongExpectedVersion.FormatWith(streamId, expectedVersion),
                                        ex);
                                }
                            }

                            return;
                        }

                        if (ex.IsUniqueConstraintViolation())
                        {
                            throw new WrongExpectedVersionException(
                                Messages.AppendFailedWrongExpectedVersion.FormatWith(streamId, expectedVersion),
                                ex);
                        }

                        throw;
                    }
                }
            }
        }

        private async Task AppendToStreamExpectedVersionNoStream(
            string streamId,
            int expectedVersion,
            NewStreamEvent[] events,
            StreamIdInfo streamIdHash,
            CancellationToken cancellationToken)
        {
            var sqlDataRecords = CreateSqlDataRecords(events);

            using (var connection = _createConnection())
            {
                await connection.OpenAsync(cancellationToken).NotOnCapturedContext();

                using(var command = new SqlCommand(Scripts.AppendStreamExpectedVersionNoStream, connection))
                {
                    command.Parameters.AddWithValue("streamId", streamIdHash.StreamId);
                    command.Parameters.AddWithValue("streamIdOriginal", streamIdHash.StreamIdOriginal);
                    var eventsParam = CreateNewEventsSqlParameter(sqlDataRecords);
                    command.Parameters.Add(eventsParam);

                    try
                    {
                        await command
                            .ExecuteNonQueryAsync(cancellationToken)
                            .NotOnCapturedContext();
                    }
                    catch(SqlException ex)
                    {
                        // Check for unique constraint violation on 
                        // https://technet.microsoft.com/en-us/library/aa258747%28v=sql.80%29.aspx
                        if(ex.IsUniqueConstraintViolationOnIndex("IX_Streams_Id"))
                        {
                            // Idempotency handling. Check if the events have already been written.

                            var page = await ReadStreamInternal(
                                    streamId,
                                    StreamPosition.Start,
                                    events.Length,
                                    ReadDirection.Forward,
                                    connection,
                                    cancellationToken)
                                    .NotOnCapturedContext();

                            if(events.Length > page.Events.Count)
                            {
                                throw new WrongExpectedVersionException(
                                    Messages.AppendFailedWrongExpectedVersion.FormatWith(streamId, expectedVersion),
                                    ex);
                            }

                            for(int i = 0; i < Math.Min(events.Length, page.Events.Count); i++)
                            {
                                if(events[i].EventId != page.Events[i].EventId)
                                {
                                    throw new WrongExpectedVersionException(
                                        Messages.AppendFailedWrongExpectedVersion.FormatWith(streamId, expectedVersion),
                                        ex);
                                }
                            }

                            return;
                        }

                        if (ex.IsUniqueConstraintViolation())
                        {
                            throw new WrongExpectedVersionException(
                                Messages.AppendFailedWrongExpectedVersion.FormatWith(streamId, expectedVersion),
                                ex);
                        }

                        throw;
                    }
                }
            }
        }

        private async Task AppendToStreamExpectedVersion(
            string streamId,
            int expectedVersion,
            NewStreamEvent[] events,
            StreamIdInfo streamIdHash,
            CancellationToken cancellationToken)
        {
            var sqlDataRecords = CreateSqlDataRecords(events);

            using (var connection = _createConnection())
            {
                await connection.OpenAsync(cancellationToken).NotOnCapturedContext();

                using(var command = new SqlCommand(Scripts.AppendStreamExpectedVersion, connection))
                {
                    command.Parameters.AddWithValue("streamId", streamIdHash.StreamId);
                    command.Parameters.AddWithValue("expectedStreamVersion", expectedVersion);
                    var eventsParam = CreateNewEventsSqlParameter(sqlDataRecords);
                    command.Parameters.Add(eventsParam);

                    try
                    {
                        await command
                            .ExecuteNonQueryAsync(cancellationToken)
                            .NotOnCapturedContext();
                    }
                    catch(SqlException ex)
                    {
                        if(ex.Errors.Count == 1)
                        {
                            var sqlError = ex.Errors[0];
                            if(sqlError.Message == "WrongExpectedVersion")
                            {
                                // Idempotency handling. Check if the events have already been written.

                                var page = await ReadStreamInternal(streamId,
                                    expectedVersion + 1, // when reading for already written events, it's from the one after the expected
                                    events.Length,
                                    ReadDirection.Forward,
                                    connection,
                                    cancellationToken);

                                if (events.Length > page.Events.Count)
                                {
                                    throw new WrongExpectedVersionException(
                                        Messages.AppendFailedWrongExpectedVersion.FormatWith(streamId, expectedVersion),
                                        ex);
                                }

                                for (int i = 0; i < Math.Min(events.Length, page.Events.Count); i++)
                                {
                                    if (events[i].EventId != page.Events[i].EventId)
                                    {
                                        throw new WrongExpectedVersionException(
                                            Messages.AppendFailedWrongExpectedVersion.FormatWith(streamId, expectedVersion),
                                            ex);
                                    }
                                }

                                return;
                            }
                        }
                        if(ex.IsUniqueConstraintViolation())
                        {
                            throw new WrongExpectedVersionException(
                                Messages.AppendFailedWrongExpectedVersion.FormatWith(streamId, expectedVersion),
                                ex);
                        }
                        throw;
                    }
                }
            }
        }

        private SqlDataRecord[] CreateSqlDataRecords(NewStreamEvent[] events)
        {
            var sqlDataRecords = events.Select(@event =>
            {
                var record = new SqlDataRecord(_appendToStreamSqlMetadata);
                record.SetGuid(1, @event.EventId);
                record.SetString(3, @event.Type);
                record.SetString(4, @event.JsonData);
                record.SetString(5, @event.JsonMetadata);
                return record;
            }).ToArray();
            return sqlDataRecords;
        }

        private static SqlParameter CreateNewEventsSqlParameter(SqlDataRecord[] sqlDataRecords)
        {
            var eventsParam = new SqlParameter("newEvents", SqlDbType.Structured)
            {
                TypeName = "dbo.NewStreamEvents",
                Value = sqlDataRecords
            };
            return eventsParam;
        }

        public Task DeleteStream(
            string streamId,
            int expectedVersion = ExpectedVersion.Any,
            CancellationToken cancellationToken = default(CancellationToken))
        {
            Ensure.That(streamId, "streamId").IsNotNullOrWhiteSpace();
            Ensure.That(expectedVersion, "expectedVersion").IsGte(-2);
            CheckIfDisposed();

            var streamIdInfo = HashStreamId(streamId);

            return expectedVersion == ExpectedVersion.Any
                ? DeleteStreamAnyVersion(streamIdInfo.StreamId, cancellationToken)
                : DeleteStreamExpectedVersion(streamIdInfo.StreamId, expectedVersion, cancellationToken);
        }

        private async Task DeleteStreamAnyVersion(
            string streamId,
            CancellationToken cancellationToken)
        {
            CheckIfDisposed();

            using (var connection = _createConnection())
            {
                await connection.OpenAsync(cancellationToken);

                using(var command = new SqlCommand(Scripts.DeleteStreamAnyVersion, connection))
                {
                    command.Parameters.AddWithValue("streamId", streamId);
                    await command
                        .ExecuteNonQueryAsync(cancellationToken)
                        .NotOnCapturedContext();
                }
            }
        }

        private async Task DeleteStreamExpectedVersion(
            string streamId,
            int expectedVersion,
            CancellationToken cancellationToken)
        {
            using (var connection = _createConnection())
            {
                await connection.OpenAsync(cancellationToken).NotOnCapturedContext();

                using(var command = new SqlCommand(Scripts.DeleteStreamExpectedVersion, connection))
                {
                    command.Parameters.AddWithValue("streamId", streamId);
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
                                string.Format(Messages.EventStreamIsDeleted, streamId),
                                ex);
                        }
                        throw;
                    }
                }
            }
        }

        public async Task<AllEventsPage> ReadAll(
            Checkpoint checkpoint,
            int maxCount,
            ReadDirection direction = ReadDirection.Forward,
            CancellationToken cancellationToken = default(CancellationToken))
        {
            Ensure.That(checkpoint, "checkpoint").IsNotNull();
            Ensure.That(maxCount, "maxCount").IsGt(0);
            CheckIfDisposed();

            long ordinal = checkpoint.GetOrdinal();

            var commandText = direction == ReadDirection.Forward ? Scripts.ReadAllForward : Scripts.ReadAllBackward;

            using(var connection = _createConnection())
            {
                await connection.OpenAsync(cancellationToken).NotOnCapturedContext();

                using(var command = new SqlCommand(commandText, connection))
                {
                    command.Parameters.AddWithValue("ordinal", ordinal);
                    command.Parameters.AddWithValue("count", maxCount + 1); //Read extra row to see if at end or not
                    var reader = await command
                        .ExecuteReaderAsync(cancellationToken)
                        .NotOnCapturedContext();

                    List<StreamEvent> streamEvents = new List<StreamEvent>();
                    if(!reader.HasRows)
                    {
                        return new AllEventsPage(checkpoint.Value,
                            null,
                            true,
                            direction,
                            streamEvents.ToArray());
                    }
                    while(await reader.ReadAsync(cancellationToken).NotOnCapturedContext())
                    {
                        var streamId = reader.GetString(0);
                        var streamVersion = reader.GetInt32(1);
                        ordinal = reader.GetInt64(2);
                        var eventId = reader.GetGuid(3);
                        var created = reader.GetDateTime(4);
                        var type = reader.GetString(5);
                        var jsonData = reader.GetString(6);
                        var jsonMetadata = reader.GetString(7);

                        var streamEvent = new StreamEvent(streamId,
                            eventId,
                            streamVersion,
                            ordinal.ToString(),
                            created,
                            type,
                            jsonData,
                            jsonMetadata);

                        streamEvents.Add(streamEvent);
                    }

                    bool isEnd = true;
                    var nextCheckpoint = streamEvents.Last().Checkpoint;

                    if(streamEvents.Count == maxCount + 1) // An extra row was read, we're not at the end
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
            var streamIdInfo = HashStreamId(streamId);

            var streamVersion = start == StreamPosition.End ? int.MaxValue : start;
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
                command.Parameters.AddWithValue("streamId", streamIdInfo.StreamId);
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
            _isDisposed.EnsureCalledOnce();
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

        private void CheckIfDisposed()
        {
            if(_isDisposed.Value)
            {
                throw new ObjectDisposedException(nameof(MsSqlEventStore));
            }
        }
    }
}