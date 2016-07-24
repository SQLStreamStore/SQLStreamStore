namespace SqlStreamStore
{
    using System.Collections.Generic;
    using System.Data.SqlClient;
    using System.Linq;
    using System.Threading;
    using System.Threading.Tasks;
    using SqlStreamStore.Streams;
    using SqlStreamStore.Infrastructure;

    public partial class MsSqlEventStore
    {
        protected override async Task<AllEventsPage> ReadAllForwardsInternal(
            long fromCheckpointExlusive,
            int maxCount,
            CancellationToken cancellationToken)
        {
            long ordinal = fromCheckpointExlusive;

            using (var connection = _createConnection())
            {
                await connection.OpenAsync(cancellationToken).NotOnCapturedContext();

                using (var command = new SqlCommand(_scripts.ReadAllForward, connection))
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
                            fromCheckpointExlusive,
                            fromCheckpointExlusive,
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
                        fromCheckpointExlusive,
                        nextCheckpoint,
                        isEnd,
                        ReadDirection.Forward,
                        streamEvents.ToArray());
                }
            }
        }

        protected override async Task<AllEventsPage> ReadAllBackwardsInternal(
            long fromCheckpointExclusive,
            int maxCount,
            CancellationToken cancellationToken = default(CancellationToken))
        {
            long ordinal = fromCheckpointExclusive == Checkpoint.End ? long.MaxValue : fromCheckpointExclusive;

            using (var connection = _createConnection())
            {
                await connection.OpenAsync(cancellationToken).NotOnCapturedContext();

                using (var command = new SqlCommand(_scripts.ReadAllBackward, connection))
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
                            Checkpoint.Start,
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

                    fromCheckpointExclusive = streamEvents.Any() ? streamEvents[0].Checkpoint : 0;

                    return new AllEventsPage(
                        fromCheckpointExclusive,
                        nextCheckpoint,
                        isEnd,
                        ReadDirection.Backward,
                        streamEvents.ToArray());
                }
            }
        }
    }
}