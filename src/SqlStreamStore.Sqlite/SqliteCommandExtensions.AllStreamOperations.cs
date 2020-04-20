namespace SqlStreamStore
{
    using System.Collections.Generic;
    using System.Data;
    using System.Threading;
    using System.Threading.Tasks;
    using Microsoft.Data.Sqlite;
    using SqlStreamStore.Streams;

    public class AllStreamOperations
    {
        private readonly SqliteConnection _connection;
        private readonly SqliteStreamStoreSettings _settings;

        public AllStreamOperations(SqliteConnection connection)
        {
            _connection = connection;
        }
        public AllStreamOperations(SqliteConnection connection, SqliteStreamStoreSettings settings) : this(connection)
        {
            _settings = settings;
        }

        public async Task<long?> ReadHeadPosition(CancellationToken cancellationToken = default)
        {
            return(await _connection.Streams("$position")
                .Properties(false, cancellationToken))
                ?.Position;
        }

        public Task<long?> RemainingInStream(ReadDirection direction, long? index)
        {
            using(var command = _connection.CreateCommand())
            {
                // determine number of remaining messages.
                command.CommandText = @"SELECT COUNT(*) 
                                        FROM messages 
                                        WHERE 
                                            CASE 
                                                WHEN @readForward THEN messages.[position] >= @position 
                                                ELSE messages.[position] <= @position
                                            END;";
                command.Parameters.Clear();
                command.Parameters.AddWithValue("@position", index);
                command.Parameters.AddWithValue("@readForward", direction == ReadDirection.Forward);
                return Task.FromResult<long?>(command.ExecuteScalar(Position.End));
            }
        }

        public Task<IReadOnlyList<StreamMessage>> Read(
            ReadDirection direction,
            long? index,
            long maxCount,
            bool prefectchMessageBody,
            CancellationToken cancellationToken = default
        )
        {
            using(var command = _connection.CreateCommand())
            {
                var messages = new List<StreamMessage>();
                command.CommandText = @"SELECT streams.id_original As stream_id,
        messages.stream_version,
        messages.position,
        messages.event_id,
        messages.created_utc,
        messages.type,
        messages.json_metadata,
        CASE WHEN @includeJsonData = true THEN messages.json_data ELSE null END
   FROM messages
INNER JOIN streams
     ON messages.stream_id_internal = streams.id_internal
  WHERE
    CASE WHEN @readForward THEN messages.position >= @position ELSE messages.position <= @position END
ORDER BY 
    CASE WHEN @readForward THEN messages.position ELSE -messages.position END
  LIMIT @count;";
                command.Parameters.Clear();
                command.Parameters.AddWithValue("@position", index);
                command.Parameters.AddWithValue("@count", maxCount);
                command.Parameters.AddWithValue("@includeJsonData", prefectchMessageBody);
                command.Parameters.AddWithValue("@readForward", direction == ReadDirection.Forward);
                var reader = command.ExecuteReader(CommandBehavior.SequentialAccess);

                while(reader.Read())
                {
                    var streamId = reader.GetString(0);
                    var streamVersion = reader.GetInt32(1);
                    var position = reader.IsDBNull(2) ? Position.End : reader.GetInt64(2);
                    var messageId = reader.GetGuid(3);
                    var created = reader.GetDateTime(4);
                    var type = reader.GetString(5);
                    var jsonMetadata = reader.GetString(6);
                    var preloadJson = (!reader.IsDBNull(7) && prefectchMessageBody)
                        ? reader.GetTextReader(7).ReadToEnd()
                        : default;

                    var message = new StreamMessage(streamId,
                        messageId,
                        streamVersion,
                        position,
                        created,
                        type,
                        jsonMetadata,
                        ct => prefectchMessageBody
                            ? Task.FromResult(preloadJson)
                            : SqliteCommandExtensions.GetJsonData(streamId, streamVersion));

                    messages.Add(message);
                }

                return Task.FromResult<IReadOnlyList<StreamMessage>>(messages);
            }
        }
    }
}