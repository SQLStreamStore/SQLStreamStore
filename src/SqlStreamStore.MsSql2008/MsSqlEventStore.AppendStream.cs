namespace StreamStore
{
    using System;
    using System.Data;
    using System.Data.SqlClient;
    using System.Linq;
    using System.Threading;
    using System.Threading.Tasks;
    using EnsureThat;
    using Microsoft.SqlServer.Server;
    using StreamStore.Infrastructure;
    using StreamStore.Streams;
    using StreamStoreStore.Json;

    public partial class MsSqlEventStore
    {
        protected override async Task AppendToStreamInternal(
           string streamId,
           int expectedVersion,
           NewStreamEvent[] events,
           CancellationToken cancellationToken)
        {
            Ensure.That(streamId, "streamId").IsNotNullOrWhiteSpace();
            Ensure.That(expectedVersion, "expectedVersion").IsGte(-2);
            Ensure.That(events, "events").IsNotNull();
            CheckIfDisposed();

            int? maxCount;
            using(var connection = _createConnection())
            {
                await connection.OpenAsync(cancellationToken).NotOnCapturedContext();
                var streamIdInfo = new StreamIdInfo(streamId);
                maxCount = await AppendToStreamInternal(connection, null, streamIdInfo.SqlStreamId, expectedVersion,
                    events, cancellationToken);
            }

            if(maxCount != null)
            {
                await CheckStreamMaxCount(streamId, maxCount, cancellationToken);
            }
        }

        private async Task<int?> AppendToStreamInternal(
           SqlConnection connection,
           SqlTransaction transaction,
           SqlStreamId sqlStreamId,
           int expectedVersion,
           NewStreamEvent[] events,
           CancellationToken cancellationToken)
        {
            CheckIfDisposed();

            if (expectedVersion == ExpectedVersion.Any)
            {
                return await AppendToStreamExpectedVersionAny(
                    connection,
                    transaction,
                    sqlStreamId,
                    events,
                    cancellationToken);
            }
            if(expectedVersion == ExpectedVersion.NoStream)
            {
                return await AppendToStreamExpectedVersionNoStream(
                    connection,
                    transaction,
                    sqlStreamId,
                    events,
                    cancellationToken);
            }
            return await AppendToStreamExpectedVersion(
                connection,
                transaction,
                sqlStreamId,
                expectedVersion,
                events,
                cancellationToken);
        }

        private async Task RetryOnDeadLock(Func<Task> operation)
        {
            Exception exception;
            do
            {
                exception = null;
                try
                {
                    await operation();
                }
                catch(SqlException ex) when(ex.Number == 1205 || ex.Number == 1222) // Deadlock error code;
                {
                    exception = ex;
                }
            } while(exception != null);
        }

        private async Task<int?> AppendToStreamExpectedVersionAny(
            SqlConnection connection,
            SqlTransaction transaction,
            SqlStreamId sqlStreamId,
            NewStreamEvent[] events,
            CancellationToken cancellationToken)
        {
            using(var command = new SqlCommand(_scripts.AppendStreamExpectedVersionAny, connection, transaction))
            {
                command.Parameters.AddWithValue("streamId", sqlStreamId.Id);
                command.Parameters.AddWithValue("streamIdOriginal", sqlStreamId.IdOriginal);
                var eventsParam = CreateNewEventsSqlParameter(CreateSqlDataRecords(events));
                command.Parameters.Add(eventsParam);

                try
                {
                    using(var reader = await command
                        .ExecuteReaderAsync(cancellationToken)
                        .NotOnCapturedContext())
                    {
                        if(await reader.ReadAsync(cancellationToken).NotOnCapturedContext())
                        {
                            var jsonData = reader.GetString(0);
                            var metadataMessage = SimpleJson.DeserializeObject<MetadataMessage>(jsonData);
                            return metadataMessage.MaxCount;
                        }
                    }
                }
                
                // Check for unique constraint violation on 
                // https://technet.microsoft.com/en-us/library/aa258747%28v=sql.80%29.aspx
                catch(SqlException ex)
                    when(ex.IsUniqueConstraintViolationOnIndex("IX_Events_StreamIdInternal_Id"))
                {
                    // Idempotency handling. Check if the events have already been written.
                    var page = await ReadStreamInternal(
                        sqlStreamId,
                        StreamVersion.Start,
                        events.Length,
                        ReadDirection.Forward,
                        connection,
                        cancellationToken)
                        .NotOnCapturedContext();

                    if(events.Length > page.Events.Length)
                    {
                        throw new WrongExpectedVersionException(
                            Messages.AppendFailedWrongExpectedVersion(sqlStreamId.IdOriginal, ExpectedVersion.Any),
                            ex);
                    }

                    for(int i = 0; i < Math.Min(events.Length, page.Events.Length); i++)
                    {
                        if(events[i].EventId != page.Events[i].EventId)
                        {
                            throw new WrongExpectedVersionException(
                                Messages.AppendFailedWrongExpectedVersion(sqlStreamId.IdOriginal, ExpectedVersion.Any),
                                ex);
                        }
                    }
                }
                catch(SqlException ex) when(ex.IsUniqueConstraintViolation())
                {
                    throw new WrongExpectedVersionException(
                        Messages.AppendFailedWrongExpectedVersion(sqlStreamId.IdOriginal, ExpectedVersion.Any),
                        ex);
                }
                return null;
            }
        }

        private async Task<int?> AppendToStreamExpectedVersionNoStream(
            SqlConnection connection,
            SqlTransaction transaction,
            SqlStreamId sqlStreamId,
            NewStreamEvent[] events,
            CancellationToken cancellationToken)
        {
            using(var command = new SqlCommand(_scripts.AppendStreamExpectedVersionNoStream, connection, transaction))
            {
                command.Parameters.AddWithValue("streamId", sqlStreamId.Id);
                command.Parameters.AddWithValue("streamIdOriginal", sqlStreamId.IdOriginal);
                var sqlDataRecords = CreateSqlDataRecords(events);
                var eventsParam = CreateNewEventsSqlParameter(sqlDataRecords);
                command.Parameters.Add(eventsParam);

                try
                {
                    using(var reader = await command
                        .ExecuteReaderAsync(cancellationToken)
                        .NotOnCapturedContext())
                    {
                        if(await reader.ReadAsync(cancellationToken).NotOnCapturedContext())
                        {
                            var jsonData = reader.GetString(0);
                            var metadataMessage = SimpleJson.DeserializeObject<MetadataMessage>(jsonData);
                            return metadataMessage.MaxCount;
                        }
                    }
                }
                catch(SqlException ex)
                {
                    // Check for unique constraint violation on 
                    // https://technet.microsoft.com/en-us/library/aa258747%28v=sql.80%29.aspx
                    if(ex.IsUniqueConstraintViolationOnIndex("IX_Streams_Id"))
                    {
                        // Idempotency handling. Check if the events have already been written.
                        var page = await ReadStreamInternal(
                            sqlStreamId,
                            StreamVersion.Start,
                            events.Length,
                            ReadDirection.Forward,
                            connection,
                            cancellationToken)
                            .NotOnCapturedContext();

                        if(events.Length > page.Events.Length)
                        {
                            throw new WrongExpectedVersionException(
                                Messages.AppendFailedWrongExpectedVersion(sqlStreamId.IdOriginal, ExpectedVersion.NoStream),
                                ex);
                        }

                        for(int i = 0; i < Math.Min(events.Length, page.Events.Length); i++)
                        {
                            if(events[i].EventId != page.Events[i].EventId)
                            {
                                throw new WrongExpectedVersionException(
                                    Messages.AppendFailedWrongExpectedVersion(sqlStreamId.IdOriginal, ExpectedVersion.NoStream),
                                    ex);
                            }
                        }

                        return null;
                    }

                    if(ex.IsUniqueConstraintViolation())
                    {
                        throw new WrongExpectedVersionException(
                            Messages.AppendFailedWrongExpectedVersion(sqlStreamId.IdOriginal, ExpectedVersion.NoStream),
                            ex);
                    }

                    throw;
                }
                return null;
            }
        }

        private async Task<int?> AppendToStreamExpectedVersion(
            SqlConnection connection,
            SqlTransaction transaction,
            SqlStreamId sqlStreamId,
            int expectedVersion,
            NewStreamEvent[] events,
            CancellationToken cancellationToken)
        {
            var sqlDataRecords = CreateSqlDataRecords(events);

            using(var command = new SqlCommand(_scripts.AppendStreamExpectedVersion, connection, transaction))
            {
                command.Parameters.AddWithValue("streamId", sqlStreamId.Id);
                command.Parameters.AddWithValue("expectedStreamVersion", expectedVersion);
                var eventsParam = CreateNewEventsSqlParameter(sqlDataRecords);
                command.Parameters.Add(eventsParam);

                try
                {
                    using (var reader = await command
                        .ExecuteReaderAsync(cancellationToken)
                        .NotOnCapturedContext())
                    {
                        if (await reader.ReadAsync(cancellationToken).NotOnCapturedContext())
                        {
                            var jsonData = reader.GetString(0);
                            var metadataMessage = SimpleJson.DeserializeObject<MetadataMessage>(jsonData);
                            return metadataMessage.MaxCount;
                        }
                    }
                }
                catch(SqlException ex)
                {
                    if(ex.Errors.Count == 1)
                    {
                        var sqlError = ex.Errors[0];
                        if(sqlError.Message == "WrongExpectedVersion")
                        {
                            // Idempotency handling. Check if the events have already been written.

                            var page = await ReadStreamInternal(
                                sqlStreamId,
                                expectedVersion + 1,
                                // when reading for already written events, it's from the one after the expected
                                events.Length,
                                ReadDirection.Forward,
                                connection,
                                cancellationToken);

                            if(events.Length > page.Events.Length)
                            {
                                throw new WrongExpectedVersionException(
                                    Messages.AppendFailedWrongExpectedVersion(sqlStreamId.IdOriginal, expectedVersion),
                                    ex);
                            }

                            for(int i = 0; i < Math.Min(events.Length, page.Events.Length); i++)
                            {
                                if(events[i].EventId != page.Events[i].EventId)
                                {
                                    throw new WrongExpectedVersionException(
                                        Messages.AppendFailedWrongExpectedVersion(sqlStreamId.IdOriginal, expectedVersion),
                                        ex);
                                }
                            }

                            return null;
                        }
                    }
                    if(ex.IsUniqueConstraintViolation())
                    {
                        throw new WrongExpectedVersionException(
                            Messages.AppendFailedWrongExpectedVersion(sqlStreamId.IdOriginal, expectedVersion),
                            ex);
                    }
                    throw;
                }
                return null;
            }
        }

        private async Task CheckStreamMaxCount(string streamId, int? maxCount, CancellationToken cancellationToken)
        {
            if (maxCount.HasValue)
            {
                var count = await GetStreamEventCount(streamId, cancellationToken);
                if (count > maxCount.Value)
                {
                    int toPurge = count - maxCount.Value;

                    var streamEventsPage = await ReadStreamForwardsInternal(streamId, StreamVersion.Start,
                        toPurge, cancellationToken);

                    if (streamEventsPage.Status == PageReadStatus.Success)
                    {
                        foreach (var streamEvent in streamEventsPage.Events)
                        {
                            await DeleteEventInternal(streamId, streamEvent.EventId, cancellationToken);
                        }
                    }
                }
            }
        }

        private SqlDataRecord[] CreateSqlDataRecords(NewStreamEvent[] events)
        {
            var dateTime = GetUtcNow().DateTime;
            var sqlDataRecords = events.Select(@event =>
            {
                var record = new SqlDataRecord(_appendToStreamSqlMetadata);
                record.SetGuid(1, @event.EventId);
                record.SetDateTime(2, dateTime);
                record.SetString(3, @event.Type);
                record.SetString(4, @event.JsonData);
                record.SetString(5, @event.JsonMetadata);
                return record;
            }).ToArray();
            return sqlDataRecords;
        }

        private SqlParameter CreateNewEventsSqlParameter(SqlDataRecord[] sqlDataRecords)
        {
            var eventsParam = new SqlParameter("newEvents", SqlDbType.Structured)
            {
                TypeName = $"{_scripts.Schema}.NewStreamEvents",
                Value = sqlDataRecords
            };
            return eventsParam;
        }
    }
}