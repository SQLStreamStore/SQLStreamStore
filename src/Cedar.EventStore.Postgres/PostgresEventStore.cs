namespace Cedar.EventStore.Postgres
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
    using Cedar.EventStore.Postgres.SqlScripts;

    using EnsureThat;

    using Npgsql;

    using NpgsqlTypes;

    public class PostgresEventStore : IEventStore
    {
        private readonly Func<NpgsqlConnection> _createConnection;

        private readonly NpgsqlConnection connection;
        private readonly InterlockedBoolean _isDisposed = new InterlockedBoolean();

        public PostgresEventStore(Func<NpgsqlConnection> createConnection)
        {
            Ensure.That(createConnection, "createConnection").IsNotNull();

            _createConnection = createConnection;
            connection = createConnection();
            connection.Open();
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

            //using(var connection = await this.OpenConnection(cancellationToken))
            using(var tx = connection.BeginTransaction(IsolationLevel.Serializable))
            {
                int streamIdInternal = -1;
                int currentVersion = expectedVersion;
                bool isDeleted = false;

                if(expectedVersion == ExpectedVersion.NoStream)
                {
                    using(
                        var command =
                            new NpgsqlCommand(
                                "INSERT INTO streams(id, id_original) VALUES (:stream_id, :stream_id_original) RETURNING id_internal;",
                                connection,
                                tx))
                    {
                        command.Parameters.AddWithValue(":stream_id", streamIdInfo.StreamId);
                        command.Parameters.AddWithValue(":stream_id_original", streamIdInfo.StreamIdOriginal);

                        streamIdInternal =
                            (int)await command.ExecuteScalarAsync(cancellationToken).NotOnCapturedContext();
                    }
                }
                else
                {
                    using(var command = new NpgsqlCommand(@"
SELECT streams.id_internal,
       streams.is_deleted,
       events.stream_version
FROM streams
LEFT JOIN events
      ON events.stream_id_internal = streams.id_internal
WHERE streams.id = :stream_id
ORDER BY events.ordinal
LIMIT 1;", connection, tx))
                    {
                        command.Parameters.AddWithValue(":stream_id", streamIdInfo.StreamId);

                        using(var dr = await command.ExecuteReaderAsync().NotOnCapturedContext())
                        {
                            while (await dr.ReadAsync().NotOnCapturedContext())
                            {
                                streamIdInternal = dr.GetInt32(0);
                                isDeleted = dr.GetBoolean(1);
                                if(!isDeleted)
                                {
                                    currentVersion = dr.GetInt32(2);
                                }
                            }
                        }
                    }

                    if(isDeleted)
                    {
                        tx.Rollback();
                        throw new StreamDeletedException(streamId);
                    }

                    if(expectedVersion != ExpectedVersion.Any && currentVersion != expectedVersion)
                    {
                        tx.Rollback();
                        throw new WrongExpectedVersionException(streamId, expectedVersion);
                    }
                }

                using(
                    var writer =
                        connection.BeginBinaryImport(
                            "COPY events (stream_id_internal, stream_version, id, created, type, json_data, json_metadata) FROM STDIN BINARY")
                    )
                {
                    foreach(var @event in events)
                    {
                        if(cancellationToken.IsCancellationRequested)
                        {
                            writer.Cancel();
                            tx.Rollback();
                        }

                        currentVersion++;
                        writer.StartRow();
                        writer.Write(streamIdInternal, NpgsqlDbType.Integer);
                        writer.Write(currentVersion, NpgsqlDbType.Integer);
                        writer.Write(@event.EventId, NpgsqlDbType.Uuid);
                        writer.Write(SystemClock.GetUtcNow(), NpgsqlDbType.TimestampTZ);
                        writer.Write(@event.Type);
                        writer.Write(@event.JsonData, NpgsqlDbType.Json);
                        writer.Write(@event.JsonMetadata, NpgsqlDbType.Json);
                    }
                }

                tx.Commit();
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
                ? this.DeleteStreamAnyVersion(streamIdInfo.StreamId, cancellationToken)
                : this.DeleteStreamExpectedVersion(streamIdInfo.StreamId, expectedVersion, cancellationToken);
        }

        private async Task DeleteStreamAnyVersion(
            string streamId,
            CancellationToken cancellationToken)
        {
            //using (var connection = await this.OpenConnection(cancellationToken))
            using (var command = new NpgsqlCommand("delete_stream_any_version", connection))
            {
                command.CommandType = CommandType.StoredProcedure;
                command.Parameters.AddWithValue("stream_id", streamId);
                await command
                    .ExecuteNonQueryAsync(cancellationToken)
                    .NotOnCapturedContext();
            }
        }


        private async Task DeleteStreamExpectedVersion(
            string streamId,
            int expectedVersion,
            CancellationToken cancellationToken)
        {
            //using (var connection = await this.OpenConnection(cancellationToken))
            using (var command = new NpgsqlCommand("delete_stream_expected_version", connection))
            {
                command.CommandType = CommandType.StoredProcedure;
                command.Parameters.AddWithValue("stream_id", streamId);
                command.Parameters.AddWithValue("expected_version", expectedVersion);
                try
                {
                    await command
                        .ExecuteNonQueryAsync(cancellationToken)
                        .NotOnCapturedContext();
                }
                catch(NpgsqlException ex)
                {
                    if(ex.MessageText == "WrongExpectedVersion")
                    {
                        throw new WrongExpectedVersionException(streamId, expectedVersion, ex);
                    }
                    throw;
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

            if(this._isDisposed.Value)
            {
                throw new ObjectDisposedException("PostgresEventStore");
            }

            long ordinal = checkpoint.GetOrdinal();

            var commandText = direction == ReadDirection.Forward ? Scripts.ReadAllForward : Scripts.ReadAllBackward;

            //using (var connection = await this.OpenConnection(cancellationToken))
            using (var command = new NpgsqlCommand(commandText, connection))
            {
                command.Parameters.AddWithValue(":ordinal", ordinal);
                command.Parameters.AddWithValue(":count", maxCount + 1); //Read extra row to see if at end or not

                List<StreamEvent> streamEvents = new List<StreamEvent>();

                using(var reader = await command.ExecuteReaderAsync(cancellationToken).NotOnCapturedContext())
                {
                    if (!reader.HasRows)
                    {
                        return new AllEventsPage(checkpoint.Value,
                            null,
                            true,
                            direction,
                            streamEvents.ToArray());
                    }

                    while (await reader.ReadAsync(cancellationToken).NotOnCapturedContext())
                    {
                        var streamId = reader.GetString(0);
                        var StreamVersion = reader.GetInt32(1);
                        ordinal = reader.GetInt64(2);
                        var eventId = reader.GetGuid(3);
                        var created = reader.GetDateTime(4);
                        var type = reader.GetString(5);
                        var jsonData = reader.GetString(6);
                        var jsonMetadata = reader.GetString(7);

                        var streamEvent = new StreamEvent(streamId,
                            eventId,
                            StreamVersion,
                            ordinal.ToString(),
                            created,
                            type,
                            jsonData,
                            jsonMetadata);

                        streamEvents.Add(streamEvent);
                    }
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

            var StreamVersion = start == StreamPosition.End ? int.MaxValue : start;
            string commandText;
            Func<List<StreamEvent>, int> getNextSequenceNumber;
            if(direction == ReadDirection.Forward)
            {
                commandText = "read_stream_forward"; //Scripts.ReadStreamForward;
                getNextSequenceNumber = events => events.Last().StreamVersion + 1;
            }
            else
            {
                commandText = "read_stream_backward"; //Scripts.ReadStreamBackward;
                getNextSequenceNumber = events => events.Last().StreamVersion - 1;
            }

            //using (var connection = await this.OpenConnection(cancellationToken))
            using (var tx = connection.BeginTransaction())
            using (var command = new NpgsqlCommand(commandText, connection, tx))
            {
                command.CommandType = CommandType.StoredProcedure;
                command.Parameters.AddWithValue(":stream_id", streamIdInfo.StreamId);
                command.Parameters.AddWithValue(":count", count + 1); //Read extra row to see if at end or not
                command.Parameters.AddWithValue(":stream_version", StreamVersion);

                List<StreamEvent> streamEvents = new List<StreamEvent>();

                using(var reader = await command.ExecuteReaderAsync(cancellationToken).NotOnCapturedContext())
                {
                    await reader.ReadAsync(cancellationToken).NotOnCapturedContext();
                    bool doesNotExist = reader.IsDBNull(0);
                    if(doesNotExist)
                    {
                        return new StreamEventsPage(
                            streamId, PageReadStatus.StreamNotFound, start, -1, -1, direction, isEndOfStream: true);
                    }


                    // Read IsDeleted result set
                    var isDeleted = reader.GetBoolean(1);
                    if(isDeleted)
                    {
                        return new StreamEventsPage(
                            streamId, PageReadStatus.StreamDeleted, 0, 0, 0, direction, isEndOfStream: true);
                    }


                    // Read Events result set
                    await reader.NextResultAsync(cancellationToken).NotOnCapturedContext();
                    while(await reader.ReadAsync(cancellationToken).NotOnCapturedContext())
                    {
                        var StreamVersion1 = reader.GetInt32(0);
                        var ordinal = reader.GetInt64(1);
                        var eventId = reader.GetGuid(2);
                        var created = reader.GetDateTime(3);
                        var type = reader.GetString(4);
                        var jsonData = reader.GetString(5);
                        var jsonMetadata = reader.GetString(6);

                        var streamEvent = new StreamEvent(
                            streamId, eventId, StreamVersion1, ordinal.ToString(), created, type, jsonData, jsonMetadata);

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
        }

        public void Dispose()
        {
            if(this._isDisposed.EnsureCalledOnce())
            {
                return;
            }
            this.connection.Dispose();
        }

        public async Task InitializeStore(
            bool ignoreErrors = false,
            CancellationToken cancellationToken = default(CancellationToken))
        {
            
            //using (var connection = await this.OpenConnection(cancellationToken))
            using(var cmd = new NpgsqlCommand(Scripts.InitializeStore, connection))
            {
                if (ignoreErrors)
                {
                    await ExecuteAndIgnoreErrors(() => cmd.ExecuteNonQueryAsync(cancellationToken))
                        .NotOnCapturedContext();
                }
                else
                {
                    await cmd.ExecuteNonQueryAsync(cancellationToken)
                        .NotOnCapturedContext();
                }
            }
        }

        public async Task DropAll(
            bool ignoreErrors = false,
            CancellationToken cancellationToken = default(CancellationToken))
        {
            //using (var connection = await this.OpenConnection(cancellationToken))
            using(var cmd = new NpgsqlCommand(Scripts.DropAll, connection))
            {
                if (ignoreErrors)
                {
                    await ExecuteAndIgnoreErrors(() => cmd.ExecuteNonQueryAsync(cancellationToken))
                        .NotOnCapturedContext();
                }
                else
                {
                    await cmd.ExecuteNonQueryAsync(cancellationToken)
                        .NotOnCapturedContext();
                }
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

        private async Task<NpgsqlConnection> OpenConnection(CancellationToken cancellationToken = default(CancellationToken))
        {
            var connection = _createConnection();
            await connection.OpenAsync(cancellationToken);
            return connection;
        }

        private class StreamIdInfo
        {
            public readonly string StreamId;
            public readonly string StreamIdOriginal;

            public StreamIdInfo(string streamId, string streamIdOriginal)
            {
                this.StreamId = streamId;
                this.StreamIdOriginal = streamIdOriginal;
            }
        }
    }
}