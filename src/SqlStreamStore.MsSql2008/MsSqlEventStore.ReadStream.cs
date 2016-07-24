namespace SqlStreamStore
{
    using System;
    using System.Collections.Generic;
    using System.Data.SqlClient;
    using System.Linq;
    using System.Threading;
    using System.Threading.Tasks;
    using SqlStreamStore.Streams;
    using SqlStreamStore.Infrastructure;

    public partial class MsSqlEventStore
    {
        protected override async Task<StreamEventsPage> ReadStreamForwardsInternal(
            string streamId,
            int start,
            int count,
            CancellationToken cancellationToken)
        {
            using(var connection = _createConnection())
            {
                await connection.OpenAsync(cancellationToken).NotOnCapturedContext();
                var streamIdInfo = new StreamIdInfo(streamId);
                return await ReadStreamInternal(streamIdInfo.SqlStreamId, start, count, ReadDirection.Forward, connection, cancellationToken);
            }
        }

        protected override async Task<StreamEventsPage> ReadStreamBackwardsInternal(
            string streamId,
            int start,
            int count,
            CancellationToken cancellationToken)
        {
            using (var connection = _createConnection())
            {
                await connection.OpenAsync(cancellationToken).NotOnCapturedContext();
                var streamIdInfo = new StreamIdInfo(streamId);
                return await ReadStreamInternal(streamIdInfo.SqlStreamId, start, count, ReadDirection.Backward, connection, cancellationToken);
            }
        }

        private async Task<StreamEventsPage> ReadStreamInternal(
            SqlStreamId sqlStreamId,
            int start,
            int count,
            ReadDirection direction,
            SqlConnection connection,
            CancellationToken cancellationToken)
        {
            // To read backwards from end, need to use int MaxValue
            var streamVersion = start == StreamVersion.End ? int.MaxValue : start;
            string commandText;
            Func<List<StreamEvent>, int> getNextSequenceNumber;
            if(direction == ReadDirection.Forward)
            {
                commandText = _scripts.ReadStreamForward;
                getNextSequenceNumber = events => events.Last().StreamVersion + 1;
            }
            else
            {
                commandText = _scripts.ReadStreamBackward;
                getNextSequenceNumber = events => events.Last().StreamVersion - 1;
            }

            using(var command = new SqlCommand(commandText, connection))
            {
                command.Parameters.AddWithValue("streamId", sqlStreamId.Id);
                command.Parameters.AddWithValue("count", count + 1); //Read extra row to see if at end or not
                command.Parameters.AddWithValue("StreamVersion", streamVersion);

                var streamEvents = new List<StreamEvent>();

                using(var reader = await command.ExecuteReaderAsync(cancellationToken).NotOnCapturedContext())
                {
                    if(!await reader.ReadAsync(cancellationToken).NotOnCapturedContext())
                    {
                        return new StreamEventsPage(
                            sqlStreamId.IdOriginal,
                            PageReadStatus.StreamNotFound,
                            start,
                            -1,
                            -1,
                            direction,
                            isEndOfStream: true);
                    }

                    // Read Events result set
                    do
                    {
                        var streamVersion1 = reader.GetInt32(0);
                        var ordinal = reader.GetInt64(1);
                        var eventId = reader.GetGuid(2);
                        var created = reader.GetDateTime(3);
                        var type = reader.GetString(4);
                        var jsonData = reader.GetString(5);
                        var jsonMetadata = reader.GetString(6);

                        var streamEvent = new StreamEvent(
                            sqlStreamId.IdOriginal,
                            eventId,
                            streamVersion1,
                            ordinal,
                            created,
                            type,
                            jsonData,
                            jsonMetadata);

                        streamEvents.Add(streamEvent);
                    } while(await reader.ReadAsync(cancellationToken).NotOnCapturedContext());

                    // Read last event revision result set
                    await reader.NextResultAsync(cancellationToken).NotOnCapturedContext();
                    await reader.ReadAsync(cancellationToken).NotOnCapturedContext();
                    var lastStreamVersion = reader.GetInt32(0);

                    var isEnd = true;
                    if(streamEvents.Count == count + 1)
                    {
                        isEnd = false;
                        streamEvents.RemoveAt(count);
                    }

                    return new StreamEventsPage(
                        sqlStreamId.IdOriginal,
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
    }
}