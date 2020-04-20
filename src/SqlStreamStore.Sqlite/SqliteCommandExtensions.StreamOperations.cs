namespace SqlStreamStore
{
    using System;
    using System.Data;
    using System.Threading;
    using System.Threading.Tasks;
    using Microsoft.Data.Sqlite;
    using SqlStreamStore.Streams;

    public class StreamOperations
    {
        private readonly SqliteConnection _connection;
        private readonly string _streamId;

        private SqliteTransaction _transaction;

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