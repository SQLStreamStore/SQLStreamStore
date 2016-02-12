namespace Cedar.EventStore
{
    using System;
    using System.Data;
    using System.Data.SqlClient;
    using System.Linq;
    using System.Threading;
    using System.Threading.Tasks;
    using Cedar.EventStore.Infrastructure;
    using Cedar.EventStore.SqlScripts;
    using Cedar.EventStore.Streams;
    using EnsureThat;
    using Microsoft.SqlServer.Server;

    public partial class MsSqlEventStore
    {
       private readonly SqlMetaData[] _appendToStreamSqlMetadata =
       {
            new SqlMetaData("StreamVersion", SqlDbType.Int, true, false, SortOrder.Unspecified, -1),
            new SqlMetaData("Id", SqlDbType.UniqueIdentifier),
            new SqlMetaData("Created", SqlDbType.DateTime, true, false, SortOrder.Unspecified, -1),
            new SqlMetaData("Type", SqlDbType.NVarChar, 128),
            new SqlMetaData("JsonData", SqlDbType.NVarChar, SqlMetaData.Max),
            new SqlMetaData("JsonMetadata", SqlDbType.NVarChar, SqlMetaData.Max),
        };

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

            var streamIdInfo = new StreamIdInfo(streamId);

            switch (expectedVersion)
            {
                case ExpectedVersion.Any:
                    return AppendToStreamExpectedVersionAny(streamIdInfo, expectedVersion, events, cancellationToken);
                case ExpectedVersion.NoStream:
                    return AppendToStreamExpectedVersionNoStream(streamId, expectedVersion, events, streamIdInfo, cancellationToken);
                default:
                    return AppendToStreamExpectedVersion(streamId, expectedVersion, events, streamIdInfo, cancellationToken);
            }
        }

        private async Task AppendToStreamExpectedVersionAny(
            StreamIdInfo streamIdInfo,
            int expectedVersion,
            NewStreamEvent[] events,
            CancellationToken cancellationToken)
        {
            var sqlDataRecords = CreateSqlDataRecords(events);

            using (var connection = _createConnection())
            {
                await connection.OpenAsync(cancellationToken).NotOnCapturedContext();

                using (var command = new SqlCommand(Scripts.AppendStreamExpectedVersionAny, connection))
                {
                    command.Parameters.AddWithValue("streamId", streamIdInfo.Hash);
                    command.Parameters.AddWithValue("streamIdOriginal", streamIdInfo.Id);
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
                                    streamIdInfo.Id,
                                    StreamVersion.Start,
                                    events.Length,
                                    ReadDirection.Forward,
                                    connection,
                                    cancellationToken)
                                    .NotOnCapturedContext();

                            if (events.Length > page.Events.Length)
                            {
                                throw new WrongExpectedVersionException(
                                    Messages.AppendFailedWrongExpectedVersion.FormatWith(streamIdInfo.Id, expectedVersion),
                                    ex);
                            }

                            for (int i = 0; i < Math.Min(events.Length, page.Events.Length); i++)
                            {
                                if (events[i].EventId != page.Events[i].EventId)
                                {
                                    throw new WrongExpectedVersionException(
                                        Messages.AppendFailedWrongExpectedVersion.FormatWith(streamIdInfo.Id, expectedVersion),
                                        ex);
                                }
                            }

                            return;
                        }

                        if (ex.IsUniqueConstraintViolation())
                        {
                            throw new WrongExpectedVersionException(
                                Messages.AppendFailedWrongExpectedVersion.FormatWith(streamIdInfo.Id, expectedVersion),
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

                using (var command = new SqlCommand(Scripts.AppendStreamExpectedVersionNoStream, connection))
                {
                    command.Parameters.AddWithValue("streamId", streamIdHash.Hash);
                    command.Parameters.AddWithValue("streamIdOriginal", streamIdHash.Id);
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
                        if (ex.IsUniqueConstraintViolationOnIndex("IX_Streams_Id"))
                        {
                            // Idempotency handling. Check if the events have already been written.

                            var page = await ReadStreamInternal(
                                    streamId,
                                    StreamVersion.Start,
                                    events.Length,
                                    ReadDirection.Forward,
                                    connection,
                                    cancellationToken)
                                    .NotOnCapturedContext();

                            if (events.Length > page.Events.Length)
                            {
                                throw new WrongExpectedVersionException(
                                    Messages.AppendFailedWrongExpectedVersion.FormatWith(streamId, expectedVersion),
                                    ex);
                            }

                            for (int i = 0; i < Math.Min(events.Length, page.Events.Length); i++)
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

                using (var command = new SqlCommand(Scripts.AppendStreamExpectedVersion, connection))
                {
                    command.Parameters.AddWithValue("streamId", streamIdHash.Hash);
                    command.Parameters.AddWithValue("expectedStreamVersion", expectedVersion);
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
                        if (ex.Errors.Count == 1)
                        {
                            var sqlError = ex.Errors[0];
                            if (sqlError.Message == "WrongExpectedVersion")
                            {
                                // Idempotency handling. Check if the events have already been written.

                                var page = await ReadStreamInternal(streamId,
                                    expectedVersion + 1, // when reading for already written events, it's from the one after the expected
                                    events.Length,
                                    ReadDirection.Forward,
                                    connection,
                                    cancellationToken);

                                if (events.Length > page.Events.Length)
                                {
                                    throw new WrongExpectedVersionException(
                                        Messages.AppendFailedWrongExpectedVersion.FormatWith(streamId, expectedVersion),
                                        ex);
                                }

                                for (int i = 0; i < Math.Min(events.Length, page.Events.Length); i++)
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
    }
}