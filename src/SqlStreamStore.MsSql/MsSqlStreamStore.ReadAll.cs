namespace SqlStreamStore
{
    using System.Collections.Generic;
    using System.Data.SqlClient;
    using System.Linq;
    using System.Threading;
    using System.Threading.Tasks;
    using SqlStreamStore.Streams;
    using SqlStreamStore.Infrastructure;

    public partial class MsSqlStreamStore
    {
        protected override async Task<AllMessagesPage> ReadAllForwardsInternal(
            long fromPositionExlusive,
            int maxCount,
            CancellationToken cancellationToken)
        {
            maxCount = maxCount == int.MaxValue ? maxCount - 1 : maxCount;
            long ordinal = fromPositionExlusive;

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

                    List<StreamMessage> messages = new List<StreamMessage>();
                    if (!reader.HasRows)
                    {
                        return new AllMessagesPage(
                            fromPositionExlusive,
                            fromPositionExlusive,
                            true,
                            ReadDirection.Forward,
                            messages.ToArray());
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

                        var message = new StreamMessage(streamId,
                            eventId,
                            streamVersion,
                            ordinal,
                            created,
                            type,
                            jsonData,
                            jsonMetadata);

                        messages.Add(message);
                    }

                    bool isEnd = true;

                    if (messages.Count == maxCount + 1) // An extra row was read, we're not at the end
                    {
                        isEnd = false;
                        messages.RemoveAt(maxCount);
                    }

                    var nextPosition = messages[messages.Count - 1].Position + 1;

                    return new AllMessagesPage(
                        fromPositionExlusive,
                        nextPosition,
                        isEnd,
                        ReadDirection.Forward,
                        messages.ToArray());
                }
            }
        }

        protected override async Task<AllMessagesPage> ReadAllBackwardsInternal(
            long fromPositionExclusive,
            int maxCount,
            CancellationToken cancellationToken = default(CancellationToken))
        {
            maxCount = maxCount == int.MaxValue ? maxCount - 1 : maxCount;
            long ordinal = fromPositionExclusive == Position.End ? long.MaxValue : fromPositionExclusive;

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

                    List<StreamMessage> messages = new List<StreamMessage>();
                    if (!reader.HasRows)
                    {
                        // When reading backwards and there are no more items, then next position is LongPosition.Start,
                        // regardles of what the fromPosition is.
                        return new AllMessagesPage(
                            Position.Start,
                            Position.Start,
                            true,
                            ReadDirection.Backward,
                            messages.ToArray());
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

                        var message = new StreamMessage(streamId,
                            eventId,
                            streamVersion,
                            ordinal,
                            created,
                            type,
                            jsonData,
                            jsonMetadata);

                        messages.Add(message);
                        lastOrdinal = ordinal;
                    }

                    bool isEnd = true;
                    var nextPosition = lastOrdinal;

                    if (messages.Count == maxCount + 1) // An extra row was read, we're not at the end
                    {
                        isEnd = false;
                        messages.RemoveAt(maxCount);
                    }

                    fromPositionExclusive = messages.Any() ? messages[0].Position : 0;

                    return new AllMessagesPage(
                        fromPositionExclusive,
                        nextPosition,
                        isEnd,
                        ReadDirection.Backward,
                        messages.ToArray());
                }
            }
        }
    }
}