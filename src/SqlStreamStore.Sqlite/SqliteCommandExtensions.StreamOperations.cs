namespace SqlStreamStore
{
    using System;
    using System.Collections.Generic;
    using System.Data;
    using System.Threading;
    using System.Threading.Tasks;
    using Microsoft.Data.Sqlite;
    using SqlStreamStore.Streams;

    public class StreamOperations
    {
        private readonly SqliteConnection _connection;
        private SqliteTransaction _transaction;
        private readonly string _streamId;

        public StreamOperations(SqliteConnection connection, string streamId)
        {
            _connection = connection;
            _streamId = streamId;
        }

        public IDisposable WithTransaction()
        {
            _transaction = _connection.BeginTransaction();
            return _transaction;
        }

        public Task Commit(CancellationToken cancellationToken = default)
        {
            _transaction.Commit();
            _transaction = null;
            return Task.CompletedTask;
        }

        public Task RollBack(CancellationToken cancellationToken = default)
        {
            _transaction.Rollback();
            _transaction = null;
            return Task.CompletedTask;
        }

        public Task<long?> AllStreamPosition(ReadDirection direction, long? version)
        {
            using(var command = CreateCommand())
            {
                command.CommandText = @"SELECT messages.position
                                    FROM messages
                                    WHERE messages.stream_id_internal = (SELECT id_internal FROM streams WHERE id_original = @streamId)
                                        AND messages.stream_version = @version;";
                command.Parameters.Clear();
                command.Parameters.AddWithValue("@streamId", _streamId);
                command.Parameters.AddWithValue("@version", version);
                command.Parameters.AddWithValue("@forwards", direction == ReadDirection.Forward);
                var position = command.ExecuteScalar<long?>();

                if(position == null)
                {
                    command.CommandText = @"SELECT CASE
                                            WHEN @forwards THEN MIN(messages.position)
                                            ELSE MAX(messages.position)
                                        END
                                        FROM messages
                                        WHERE messages.stream_id_internal = (SELECT id_internal FROM streams WHERE id_original = @streamId) 
                                            AND CASE
                                                WHEN @forwards THEN messages.stream_version >= @version
                                                ELSE messages.stream_version <= @version
                                            END;";
                    command.Parameters.Clear();
                    command.Parameters.AddWithValue("@streamId", _streamId);
                    command.Parameters.AddWithValue("@version", version);
                    command.Parameters.AddWithValue("@forwards", direction == ReadDirection.Forward);
                    position = command.ExecuteScalar<long?>(long.MaxValue);
                }

                return Task.FromResult(position);
            }
        }

        public Task<long?> AllStreamPosition(ReadDirection direction, Guid eventId)
        {
            using(var command = CreateCommand())
            {
                command.CommandText = @"SELECT messages.position
                                    FROM messages
                                    WHERE messages.stream_id_internal = (SELECT id_internal FROM streams WHERE id_original = @streamId)
                                        AND messages.event_id = @eventId;";
                command.Parameters.Clear();
                command.Parameters.AddWithValue("@streamId", _streamId);
                command.Parameters.AddWithValue("@eventId", eventId);
                command.Parameters.AddWithValue("@forwards", direction == ReadDirection.Forward);
                return Task.FromResult(command.ExecuteScalar<long?>());
            }
        }

        public Task<bool> Contains(Guid eventId)
        {
            using(var command = CreateCommand())
            {
                // if the message's event id exists in the database...
                command.CommandText = @"SELECT count(*) 
                                        FROM messages
                                        WHERE event_id = @eventId AND stream_id_internal = (SELECT id_internal FROM streams WHERE id_original = @streamId);";
                command.Parameters.Clear();
                command.Parameters.AddWithValue("@streamId", _streamId);
                command.Parameters.AddWithValue("@eventId", eventId);

                return Task.FromResult(command.ExecuteScalar<long>(0) > 0);
            }
        }

        public Task<bool> Exists()
        {
            using(var command = CreateCommand())
            {
                command.CommandText = @"SELECT COUNT(*)
                                        FROM streams
                                        WHERE id_original = @streamId;";
                command.Parameters.AddWithValue("@streamId", _streamId);
                return Task.FromResult(command.ExecuteScalar<long?>(0) > 0);
            }
        }

        public Task<bool> Exists(Guid eventId, long? expected)
        {
            using(var command = CreateCommand())
            {
                command.CommandText = @"SELECT COUNT(*)
                                        FROM messages
                                        WHERE messages.stream_id_internal = (SELECT id_internal FROM streams WHERE id_original = @streamId)
                                            AND messages.stream_version <= @expected
                                            AND messages.event_id = @eventId;";
                command.Parameters.Clear();
                command.Parameters.AddWithValue("@streamId", _streamId);
                command.Parameters.AddWithValue("@expected", expected + 1);
                command.Parameters.AddWithValue("@eventId", eventId);
                return Task.FromResult(command.ExecuteScalar<int?>(0) > 0);
            }
        }
        
        public Task<bool> ExistsAtExpectedPosition(Guid eventId, long? expected)
        {
            using(var command = CreateCommand())
            {
                command.CommandText = @"SELECT COUNT(*)
                                        FROM messages
                                        WHERE messages.stream_id_internal = (SELECT id_internal FROM streams WHERE id_original = @streamId)
                                            AND messages.stream_version = @expected
                                            AND messages.event_id = @eventId;";
                command.Parameters.Clear();
                command.Parameters.AddWithValue("@streamId", _streamId);
                command.Parameters.AddWithValue("@expected", expected + 1);
                command.Parameters.AddWithValue("@eventId", eventId);
                return Task.FromResult(command.ExecuteScalar<int?>(0) > 0);
            }
        }
        
        public Task<int> Length(CancellationToken cancellationToken)
        {
            using(var command = CreateCommand())
            {
                command.CommandText = @"SELECT COUNT(*)
                                        FROM messages
                                        JOIN streams on messages.stream_id_internal = streams.id_internal
                                        WHERE streams.id_original = @idOriginal";
                command.Parameters.AddWithValue("@idOriginal", _streamId);

                return Task.FromResult(command.ExecuteScalar(0));
            }
        }

        public Task<int> Length(ReadDirection direction, long? startingIndex, CancellationToken cancellationToken)
        {
            using(var command = CreateCommand())
            {
                command.CommandText = @"SELECT COUNT(*)
                                        FROM messages 
                                        WHERE messages.stream_id_internal = (SELECT id_internal FROM streams WHERE id_original = @streamId LIMIT 1) 
                                        AND CASE 
                                                WHEN @forwards THEN messages.[position] >= @position
                                                ELSE messages.[position] <= @position
                                            END; -- count of remaining messages.";
                command.Parameters.Clear();
                command.Parameters.AddWithValue("@streamId", _streamId);
                command.Parameters.AddWithValue("@position", startingIndex);
                command.Parameters.AddWithValue("@forwards", direction == ReadDirection.Forward);
                return Task.FromResult(command.ExecuteScalar<int>());
            }
        }

        public Task<StreamHeader> Properties(bool initializeIfNotFound = true, CancellationToken cancellationToken = default)
        {
            using(var command = CreateCommand())
            {
                command.CommandText = @"SELECT id, id_internal, [version], [position], max_age, max_count
                                    FROM streams
                                    WHERE streams.id_original = @streamId;";
                command.Parameters.Clear();
                command.Parameters.AddWithValue("@streamId", _streamId);

                using(var reader = command.ExecuteReader(CommandBehavior.SingleRow))
                {
                    if(!reader.Read())
                        return initializeIfNotFound ? InitializePositionStream() : Task.FromResult<StreamHeader>(default);
                    
                    var props = new StreamHeader
                    {
                        Id = reader.ReadScalar<string>(0),
                        Key = reader.ReadScalar<int>(1),
                        Version = reader.ReadScalar<int>(2),
                        Position = reader.ReadScalar<int>(3),
                        MaxAge = reader.ReadScalar<int?>(4),
                        MaxCount = reader.ReadScalar<int>(5),
                    };

                    return Task.FromResult(props);
                }
            }
        }

        public Task<IReadOnlyList<StreamMessage>> Read(
            ReadDirection direction,
            long? position,
            bool prefetchJsonData,
            int maxRecords)
        {
            var messages = new List<StreamMessage>();
            
            using(var command = CreateCommand())
            {
                command.CommandText = @"SELECT messages.event_id,
                               messages.stream_version,
                               messages.[position],
                               messages.created_utc,
                               messages.[type],
                               messages.json_metadata,
                               case when @prefetch then messages.json_data else null end as json_data
                        FROM messages
                        WHERE messages.stream_id_internal = (SELECT id_internal FROM streams WHERE id_original = @streamId LIMIT 1)
                        AND CASE 
                                WHEN @forwards THEN messages.[position] >= @position
                                ELSE messages.[position] <= @position
                            END
                        ORDER BY
                            CASE 
                                WHEN @forwards THEN messages.position
                                ELSE -messages.position
                            END
                        LIMIT @count;";
                command.Parameters.Clear();
                command.Parameters.AddWithValue("@streamId", _streamId);
                command.Parameters.AddWithValue("@position", position);
                command.Parameters.AddWithValue("@prefetch", prefetchJsonData);
                command.Parameters.AddWithValue("@count", maxRecords);
                command.Parameters.AddWithValue("@forwards", direction == ReadDirection.Forward);

                using(var reader = command.ExecuteReader())
                {
                    while(reader.Read())
                    {
                        var preloadJson = (!reader.IsDBNull(6) && prefetchJsonData)
                            ? reader.GetTextReader(6).ReadToEnd()
                            : default;
                        var streamVersion = reader.ReadScalar<int>(1);  
                        
                        var msg = new StreamMessage(
                            _streamId,
                            reader.ReadScalar<Guid>(0, Guid.Empty),
                            streamVersion,
                            reader.ReadScalar<long>(2),
                            reader.ReadScalar<DateTime>(3),
                            reader.ReadScalar<string>(4),
                            reader.ReadScalar<string>(5),
                            ct => prefetchJsonData
                                ? Task.FromResult(preloadJson)
                                : SqliteCommandExtensions.GetJsonData(_streamId, streamVersion)
                        );
                        messages.Add(msg);
                    }
                }
                
                return Task.FromResult<IReadOnlyList<StreamMessage>>(messages);
            }
        }

        public Task<bool> RemoveEvent(Guid eventId, CancellationToken cancellationToken = default)
        {
            bool streamExists = false;
            bool willBeDeleted = false;

            using(var command = CreateCommand())
            {
                command.CommandText = @"SELECT COUNT(*) FROM streams WHERE id_original = @streamId;";
                command.Parameters.Clear();
                command.Parameters.AddWithValue("@streamId", _streamId);
                streamExists= command.ExecuteScalar<int>() > 0;

                command.CommandText = @"SELECT COUNT(*)
FROM messages
    JOIN streams ON messages.stream_id_internal = streams.id_internal 
WHERE streams.id_original = @streamId
    AND messages.event_id = @messageId;";
                command.Parameters.Clear();
                command.Parameters.AddWithValue("@streamId", _streamId);
                command.Parameters.AddWithValue("@messageId", eventId);
                willBeDeleted = command.ExecuteScalar<int>() > 0;

                if(streamExists && willBeDeleted)
                {
                    command.CommandText = @"DELETE FROM messages WHERE messages.event_id = @messageId;";
                    command.Parameters.Clear();
                    command.Parameters.AddWithValue("@messageId", eventId);
                    command.ExecuteNonQuery();
                }
            }

            return Task.FromResult(willBeDeleted && streamExists);
        } 
        
        private Task<StreamHeader> InitializePositionStream()
        {
            var idInfo = new StreamIdInfo(_streamId);
            int rowId = 0;
            using(var command = _connection.CreateCommand())
            {
                command.CommandText = @"INSERT INTO streams (id, id_original, [position])
                                    VALUES(@id, @idOriginal, @position);
                                    SELECT last_insert_rowid();";
                command.Parameters.Clear();
                command.Parameters.AddWithValue("@id", idInfo.SqlStreamId.Id);
                command.Parameters.AddWithValue("@idOriginal", idInfo.SqlStreamId.IdOriginal);
                command.Parameters.AddWithValue("@position", Position.End);
                rowId = command.ExecuteScalar<int>();
            }

            return Task.FromResult(new StreamHeader
            {
                Id = idInfo.SqlStreamId.Id,
                Key = rowId,
                Version = StreamVersion.End,
                Position =  Position.End,
                MaxAge = default,
                MaxCount = default
            });
        }

        private SqliteCommand CreateCommand()
        {
            var cmd = _connection.CreateCommand();
            
            if(_transaction != null)
            {
                cmd.Transaction = _transaction;
            }

            return cmd;
        }
    }
}