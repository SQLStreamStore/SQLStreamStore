namespace SqlStreamStore
{
    using System;
    using System.Collections.Generic;
    using System.Data;
    using System.Threading;
    using System.Threading.Tasks;
    using Microsoft.Data.Sqlite;
    using SqlStreamStore.Streams;

    public class AllStreamOperations
    {
        private readonly SqliteConnection _connection;
        private SqliteTransaction _transaction;

        public AllStreamOperations(SqliteConnection connection)
        {
            _connection = connection;
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

        public async Task<(bool StreamDeleted, bool MetadataDeleted)> Delete(string streamId, int expected, CancellationToken cancellationToken = default)
        {
            using(var command = CreateCommand())
            {
                var info = new StreamIdInfo(streamId);
                var stream = await DeleteStreamPart(command, info.SqlStreamId.IdOriginal, expected, cancellationToken);
                var metadata = await DeleteStreamPart(command, info.MetadataSqlStreamId.IdOriginal, ExpectedVersion.Any, cancellationToken);

                return (stream, metadata);
            }
        }

        public Task<bool> DeleteStreamPart(SqliteCommand command, string streamId, int expected, CancellationToken cancellationToken)
        {
            if(expected != ExpectedVersion.Any)
            {
                command.CommandText = @"SELECT messages.stream_version 
                                    FROM messages 
                                    WHERE messages.stream_id_internal = (SELECT id_internal FROM streams WHERE streams.id_original = @streamId)
                                    ORDER BY messages.stream_version DESC 
                                    LIMIT 1;";
                command.Parameters.Clear();
                command.Parameters.AddWithValue("@streamId", streamId);
                var currentVersion = command.ExecuteScalar<long?>();

                if(currentVersion != expected)
                {
                    throw new WrongExpectedVersionException(
                        ErrorMessages.DeleteStreamFailedWrongExpectedVersion(streamId,
                            expected),
                        streamId,
                        expected
                    );
                }
            }
            
            // delete stream records.
            command.CommandText =
                @"SELECT COUNT(*) FROM messages WHERE messages.stream_id_internal = (SELECT id_internal FROM streams WHERE streams.id_original = @streamId);
                                SELECT COUNT(*) FROM streams WHERE streams.id_original = @streamId;
                                DELETE FROM messages          WHERE messages.stream_id_internal = (SELECT id_internal FROM streams WHERE streams.id_original = @streamId);
                                DELETE FROM streams WHERE streams.id_original = @streamId;";
            command.Parameters.Clear();
            command.Parameters.AddWithValue("@streamId", streamId);
            using(var reader = command.ExecuteReader())
            {
                reader.Read();
                var numberOfMessages = reader.ReadScalar<int?>(0);

                reader.NextResult();
                reader.Read();
                var numberOfStreams = reader.ReadScalar<int?>(0);
                    
                return Task.FromResult(numberOfMessages + numberOfStreams > 0);
            }
        }

        public async Task<long?> HeadPosition(CancellationToken cancellationToken = default)
        {
            return(await _connection.Streams("$position")
                .Properties(false, cancellationToken))
                ?.Position;
        }

        public Task<IReadOnlyList<StreamHeader>> List(
            Pattern pattern,
            int maxCount,
            string continuationToken,
            CancellationToken cancellationToken = default)
        {
            //RESEARCH: Can this cause some sort of DDoS attack?
            if(!int.TryParse(continuationToken, out var id))
            {
                id = 0;
            }

            if(id == -1)
            {
                return Task.FromResult<IReadOnlyList<StreamHeader>>(new List<StreamHeader>());
            }

            var headers = new List<StreamHeader>();
            using(var command = CreateCommand())
            {
                command.CommandText = @"SELECT id, id_internal, id_original, [version], [position], max_age, max_count
                                    FROM streams
                                    WHERE streams.id_original >= @id;";
                switch(pattern)
                {
                    case Pattern.StartingWith _:
                        command.CommandText += "\n AND streams.id_original LIKE CONCAT(@Pattern), '%')";
                        break;
                    case Pattern.EndingWith _:
                        command.CommandText += "\n AND streams.id_original LIKE CONCAT('%', @Pattern)";
                        break;
                }
                command.CommandText += "\n LIMIT @maxCount;";

                command.Parameters.Clear();
                command.Parameters.AddWithValue("@id", id);
                command.Parameters.AddWithValue("@pattern", pattern);
                command.Parameters.AddWithValue("@maxCount", maxCount);

                using(var reader = command.ExecuteReader(CommandBehavior.SingleRow))
                {
                    while(reader.Read())
                    {
                        headers.Add(new StreamHeader
                        {
                            Id = reader.ReadScalar<string>(0),
                            Key = reader.ReadScalar<int>(1),
                            IdOriginal =  reader.ReadScalar<string>(2),
                            Version = reader.ReadScalar<int>(3),
                            Position = reader.ReadScalar<int>(4),
                            MaxAge = reader.ReadScalar<int?>(5),
                            MaxCount = reader.ReadScalar<int>(6),
                        });
                    }
                }
            }

            return Task.FromResult<IReadOnlyList<StreamHeader>>(headers);
        }

        public Task<long?> Remaining(ReadDirection direction, long? index)
        {
            using(var command = CreateCommand())
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
            using(var command = CreateCommand())
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