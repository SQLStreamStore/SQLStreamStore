namespace Cedar.EventStore
{
    using System;
    using System.Collections.Generic;
    using System.Data.SqlClient;
    using System.Linq;
    using System.Threading;
    using System.Threading.Tasks;
    using Cedar.EventStore.Infrastructure;
    using Cedar.EventStore.SqlScripts;
    using Cedar.EventStore.Streams;

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

                return await ReadStreamInternal(streamId, start, count, ReadDirection.Forward, connection, cancellationToken);
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

                return await ReadStreamInternal(streamId, start, count, ReadDirection.Backward, connection, cancellationToken);
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
            var streamIdInfo = new StreamIdInfo(streamId);

            var streamVersion = start == StreamVersion.End ? int.MaxValue : start;
                // To read backwards from end, need to use int MaxValue
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
                command.Parameters.AddWithValue("streamId", streamIdInfo.Hash);
                command.Parameters.AddWithValue("count", count + 1); //Read extra row to see if at end or not
                command.Parameters.AddWithValue("StreamVersion", streamVersion);

                var streamEvents = new List<StreamEvent>();

                var reader = await command.ExecuteReaderAsync(cancellationToken).NotOnCapturedContext();
                await reader.ReadAsync(cancellationToken).NotOnCapturedContext();
                var doesNotExist = reader.IsDBNull(0);
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
                        ordinal,
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

                var isEnd = true;
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
}