namespace Cedar.EventStore
{
    using System.Collections.Generic;
    using System.Data.SqlClient;
    using System.Threading;
    using System.Threading.Tasks;
    using Cedar.EventStore.Infrastructure;
    using Cedar.EventStore.SqlScripts;
    using Cedar.EventStore.Streams;
    using EnsureThat;

    public partial class MsSqlEventStore
    {
        public Task<AllEventsPage> ReadAll(
            long fromCheckpoint,
            int maxCount,
            ReadDirection direction = ReadDirection.Forward,
            CancellationToken cancellationToken = default(CancellationToken))
        {
            Ensure.That(maxCount, nameof(maxCount)).IsGt(0).And().IsLte(1000);
            CheckIfDisposed();

            return direction == ReadDirection.Forward
                ? ReadAllForwards(fromCheckpoint, maxCount, cancellationToken)
                : ReadAllBackwards(fromCheckpoint, maxCount, cancellationToken);
        }

        private async Task<AllEventsPage> ReadAllForwards(
            long fromCheckpoint,
            int maxCount,
            CancellationToken cancellationToken = default(CancellationToken))
        {
            Ensure.That(maxCount, nameof(maxCount)).IsGt(0).And().IsLte(1000);
            CheckIfDisposed();

            long ordinal = fromCheckpoint;

            using (var connection = _createConnection())
            {
                await connection.OpenAsync(cancellationToken).NotOnCapturedContext();

                using (var command = new SqlCommand(Scripts.ReadAllForward, connection))
                {
                    command.Parameters.AddWithValue("ordinal", ordinal);
                    command.Parameters.AddWithValue("count", maxCount + 1); //Read extra row to see if at end or not
                    var reader = await command
                        .ExecuteReaderAsync(cancellationToken)
                        .NotOnCapturedContext();

                    List<StreamEvent> streamEvents = new List<StreamEvent>();
                    if (!reader.HasRows)
                    {
                        return new AllEventsPage(
                            fromCheckpoint,
                            fromCheckpoint,
                            true,
                            ReadDirection.Forward,
                            streamEvents.ToArray());
                    }

                    long lastOrdinal = 0;
                    while (await reader.ReadAsync(cancellationToken).NotOnCapturedContext())
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
                            ordinal,
                            created,
                            type,
                            jsonData,
                            jsonMetadata);

                        streamEvents.Add(streamEvent);
                    }

                    bool isEnd = true;

                    if (streamEvents.Count == maxCount + 1) // An extra row was read, we're not at the end
                    {
                        isEnd = false;
                        streamEvents.RemoveAt(maxCount);
                    }

                    var nextCheckpoint = streamEvents[streamEvents.Count - 1].Checkpoint + 1;

                    return new AllEventsPage(
                        fromCheckpoint,
                        nextCheckpoint,
                        isEnd,
                        ReadDirection.Forward,
                        streamEvents.ToArray());
                }
            }
        }

        private async Task<AllEventsPage> ReadAllBackwards(
            long fromCheckpoint,
            int maxCount,
            CancellationToken cancellationToken = default(CancellationToken))
        {
            Ensure.That(maxCount, nameof(maxCount)).IsGt(0).And().IsLte(1000);
            CheckIfDisposed();

            long ordinal = fromCheckpoint == Checkpoint.End ? long.MaxValue : fromCheckpoint;

            using (var connection = _createConnection())
            {
                await connection.OpenAsync(cancellationToken).NotOnCapturedContext();

                using (var command = new SqlCommand(Scripts.ReadAllBackward, connection))
                {
                    command.Parameters.AddWithValue("ordinal", ordinal);
                    command.Parameters.AddWithValue("count", maxCount + 1); //Read extra row to see if at end or not
                    var reader = await command
                        .ExecuteReaderAsync(cancellationToken)
                        .NotOnCapturedContext();

                    List<StreamEvent> streamEvents = new List<StreamEvent>();
                    if (!reader.HasRows)
                    {
                        // When reading backwards and there are no more items, then next checkpoint is LongCheckpoint.Start,
                        // regardles of what the fromCheckpoint is.
                        return new AllEventsPage(
                            fromCheckpoint,
                            Checkpoint.Start,
                            true,
                            ReadDirection.Backward,
                            streamEvents.ToArray());
                    }

                    long lastOrdinal = 0;
                    while (await reader.ReadAsync(cancellationToken).NotOnCapturedContext())
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
                            ordinal,
                            created,
                            type,
                            jsonData,
                            jsonMetadata);

                        streamEvents.Add(streamEvent);
                        lastOrdinal = ordinal;
                    }

                    bool isEnd = true;
                    var nextCheckpoint = lastOrdinal;

                    if (streamEvents.Count == maxCount + 1) // An extra row was read, we're not at the end
                    {
                        isEnd = false;
                        streamEvents.RemoveAt(maxCount);
                    }

                    return new AllEventsPage(
                        fromCheckpoint,
                        nextCheckpoint,
                        isEnd,
                        ReadDirection.Backward,
                        streamEvents.ToArray());
                }
            }
        }
    }
}