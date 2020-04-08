namespace SqlStreamStore
{
    using System;
    using System.Collections.Generic;
    using System.Threading;
    using System.Threading.Tasks;
    using Microsoft.Data.Sqlite;
    using SqlStreamStore.Streams;

    public partial class SqliteStreamStore
    {
        protected override Task<ReadStreamPage> ReadStreamForwardsInternal(
            string streamId,
            int start,
            int count,
            bool prefetch,
            ReadNextStreamPage readNext,
            CancellationToken cancellationToken)
        {
            GuardAgainstDisposed();
            cancellationToken.ThrowIfCancellationRequested();

            // If the count is int.MaxValue, TSql will see it as a negative number. 
            // Users shouldn't be using int.MaxValue in the first place anyway.
            count = count == int.MaxValue ? count - 1 : count;
            var streamVersion = start == StreamVersion.End ? int.MaxValue : start;
            using (var connection = OpenConnection())
            using (var command = connection.CreateCommand())
            {
                var idInfo = new StreamIdInfo(streamId);
                int streamIdInternal = 0;
                int lastStreamVersion = 0;
                int lastStreamPosition = 0;
                var maxAge = default(int?);
                command.CommandText = @"SELECT streams.id_internal, 
                                                streams.[version], 
                                                streams.[position], 
                                                coalesce(max_age, (SELECT s.max_age FROM streams s WHERE s.id = @metaId)), 
                                                coalesce(max_count, (SELECT s.max_count FROM streams s WHERE s.id = @metaId))
                                        FROM streams
                                        where streams.id = @streamId";
                command.Parameters.Clear();
                command.Parameters.AddWithValue("@streamId", idInfo.SqlStreamId.Id);
                command.Parameters.AddWithValue("@metaId", idInfo.MetadataSqlStreamId.Id);
                using(var reader = command.ExecuteReader())
                {
                    if(!reader.Read())
                    {
                        // not found.
                        return Task.FromResult(new ReadStreamPage(
                            streamId,
                            PageReadStatus.StreamNotFound,
                            start,
                            StreamVersion.End,
                            StreamVersion.End,
                            StreamVersion.End,
                            ReadDirection.Forward,
                            true,
                            readNext));
                    }

                    streamIdInternal = reader.GetInt32(0);
                    lastStreamVersion = reader.GetInt32(1);
                    lastStreamPosition = reader.GetInt32(2);
                    maxAge = reader.IsDBNull(3) ? default(int?) : reader.GetInt32(3);
                    if(!reader.IsDBNull(4))
                    {
                        count = reader.GetInt32(4);
                    }
                }

                long? position;
                command.CommandText = @"SELECT messages.position
                                        FROM messages
                                        WHERE messages.stream_id_internal = @streamIdInternal
                                            AND messages.stream_version = @streamVersion;";
                command.Parameters.Clear();
                command.Parameters.AddWithValue("@streamIdInternal", streamIdInternal);
                command.Parameters.AddWithValue("@streamVersion", streamVersion);
                position = command.ExecuteScalar<long?>();

                if(position == null)
                {
                    command.CommandText = @"SELECT MIN(messages.position)
                                            FROM messages
                                            WHERE messages.stream_id_internal = @streamIdInternal
                                                AND messages.stream_version >= @streamVersion;";
                    command.Parameters.Clear();
                    command.Parameters.AddWithValue("@streamIdInternal", streamIdInternal);
                    command.Parameters.AddWithValue("@streamVersion", lastStreamVersion);
                    position = command.ExecuteScalar<long?>() ?? Position.Start;
                }

                command.CommandText = @"SELECT COUNT(*) 
                        FROM messages 
                        WHERE messages.stream_id_internal = @streamIdInternal AND position >= @position; 
                        SELECT messages.event_id,
                               messages.stream_version,
                               messages.[position],
                               messages.created_utc,
                               messages.type,
                               messages.json_metadata,
                               case when @prefetch then messages.json_data else null end as json_data
                        FROM messages
                        WHERE messages.stream_id_internal = @streamIdInternal AND messages.position >= @position
                        ORDER BY messages.position
                        LIMIT @count;";
                command.Parameters.AddWithValue("@streamIdInternal", streamIdInternal);
                command.Parameters.AddWithValue("@position", position);
                command.Parameters.AddWithValue("@prefetch", prefetch);
                command.Parameters.AddWithValue("@count", count + 1);

                var messages = new List<(StreamMessage message, int? maxAge)>();
                using(var reader = command.ExecuteReader())
                {
                    reader.Read();
                    var remainingMessages = Convert.ToInt32(reader.GetValue(0));

                    reader.NextResult();
                    while(reader.Read() || messages.Count == count)
                    {
                        if(messages.Count == count)
                        {
                            lastStreamVersion = reader.GetInt32(1);
                            lastStreamPosition = reader.GetInt32(2);
                            continue;
                        }
                        
                        messages.Add((ReadStreamMessage(reader, idInfo, prefetch), maxAge));
                    }

                    var filtered = FilterExpired(messages);
                    var isEnd = remainingMessages - messages.Count <= 0;
                    
                    var nextVersion = isEnd
                    ? StreamVersion.End
                    : _resolveNextVersion(filtered, ReadDirection.Forward, lastStreamVersion);
                    
                    var page = new ReadStreamPage(
                        streamId,
                        PageReadStatus.Success,
                        start,
                        nextVersion,
                        lastStreamVersion,
                        lastStreamPosition,
                        ReadDirection.Forward,
                        isEnd,
                        readNext,
                        filtered.ToArray());
            
                    return Task.FromResult(page);
                }
            }
        }

        protected override Task<ReadStreamPage> ReadStreamBackwardsInternal(
            string streamId,
            int fromVersionInclusive,
            int count,
            bool prefetch,
            ReadNextStreamPage readNext,
            CancellationToken cancellationToken)
        {
            GuardAgainstDisposed();
            cancellationToken.ThrowIfCancellationRequested();

            // If the count is int.MaxValue, TSql will see it as a negative number. 
            // Users shouldn't be using int.MaxValue in the first place anyway.
            count = count == int.MaxValue ? count - 1 : count;
            var streamVersion = fromVersionInclusive == StreamVersion.End ? int.MaxValue -1 : fromVersionInclusive;
            using (var connection = OpenConnection())
            using (var command = connection.CreateCommand())
            {
                var idInfo = new StreamIdInfo(streamId);
                int streamIdInternal = 0;
                int startStreamVersion = 0;
                int lastStreamVersion = 0;
                int lastStreamPosition = 0;
                var maxAge = default(int?);
                command.CommandText = @"SELECT streams.id_internal, 
                                                streams.[version], 
                                                streams.[position], 
                                                coalesce(max_age, (SELECT s.max_age FROM streams s WHERE s.id = @metaId)), 
                                                coalesce(max_count, (SELECT s.max_count FROM streams s WHERE s.id = @metaId))
                                        FROM streams
                                        where streams.id = @streamId";
                command.Parameters.Clear();
                command.Parameters.AddWithValue("@streamId", idInfo.SqlStreamId.Id);
                command.Parameters.AddWithValue("@metaId", idInfo.MetadataSqlStreamId.Id);
                using(var reader = command.ExecuteReader())
                {
                    if(!reader.Read())
                    {
                        // not found.
                        return Task.FromResult(new ReadStreamPage(
                            streamId,
                            PageReadStatus.StreamNotFound,
                            fromVersionInclusive,
                            StreamVersion.End,
                            StreamVersion.End,
                            StreamVersion.End,
                            ReadDirection.Backward,
                            true,
                            readNext));
                    }

                    streamIdInternal = reader.GetInt32(0);
                    lastStreamVersion = reader.GetInt32(1);
                    startStreamVersion = lastStreamPosition;
                    lastStreamPosition = reader.GetInt32(2);
                    maxAge = reader.IsDBNull(3) ? default(int?) : reader.GetInt32(3);
                    if(!reader.IsDBNull(4))
                    {
                        count = reader.GetInt32(4);
                    }
                }

                long? position;
                command.CommandText = @"SELECT messages.position
                                        FROM messages
                                        WHERE messages.stream_id_internal = @streamIdInternal
                                            AND messages.stream_version = @streamVersion;";
                command.Parameters.Clear();
                command.Parameters.AddWithValue("@streamIdInternal", streamIdInternal);
                command.Parameters.AddWithValue("@streamVersion", streamVersion);
                position = command.ExecuteScalar<long?>();

                if(position == null)
                {
                    command.CommandText = @"SELECT MIN(messages.position)
                                            FROM messages
                                            WHERE messages.stream_id_internal = @streamIdInternal
                                                AND messages.stream_version >= @streamVersion;";
                    command.Parameters.Clear();
                    command.Parameters.AddWithValue("@streamIdInternal", streamIdInternal);
                    command.Parameters.AddWithValue("@streamVersion", lastStreamVersion);
                    position = command.ExecuteScalar<long?>() ?? Position.Start;
                }

                command.CommandText = @"SELECT COUNT(*) 
                        FROM messages 
                        WHERE messages.stream_id_internal = @idInternal AND messages.[stream_version] <= @version;
                         
                        SELECT messages.event_id,
                               messages.stream_version,
                               messages.[position],
                               messages.created_utc,
                               messages.[type],
                               messages.json_metadata,
                               case when @prefetch then messages.json_data else null end as json_data
                        FROM messages
                        WHERE messages.stream_id_internal = @idInternal AND messages.[stream_version] <= @version
                        ORDER BY messages.position DESC
                        LIMIT @count;";
                command.Parameters.AddWithValue("@idInternal", streamIdInternal);
                command.Parameters.AddWithValue("@version", streamVersion);
                command.Parameters.AddWithValue("@prefetch", prefetch);
                command.Parameters.AddWithValue("@count", count + 1);

                var messages = new List<(StreamMessage message, int? maxAge)>();
                using(var reader = command.ExecuteReader())
                {
                    reader.Read();
                    var remainingMessages = Convert.ToInt32(reader.GetValue(0));

                    reader.NextResult();
                    while(reader.Read() || messages.Count == count)
                    {
                        if(messages.Count == count)
                        {
                            lastStreamVersion = reader.GetInt32(1);
                            lastStreamPosition = reader.GetInt32(2);
                            continue;
                        }
                        
                        messages.Add((ReadStreamMessage(reader, idInfo, prefetch), maxAge));
                    }

                    var filtered = FilterExpired(messages);
                    var isEnd = remainingMessages - messages.Count <= 0;
                    var nextVersion = _resolveNextVersion(filtered, ReadDirection.Backward, startStreamVersion);
                    
                    var page = new ReadStreamPage(
                        streamId,
                        PageReadStatus.Success,
                        fromVersionInclusive,
                        nextVersion,
                        lastStreamVersion,
                        lastStreamPosition,
                        ReadDirection.Backward,
                        isEnd,
                        readNext,
                        filtered.ToArray());
            
                    return Task.FromResult(page);
                }
            }
        }
 
        private StreamMessage ReadStreamMessage(SqliteDataReader reader, StreamIdInfo idInfo, bool prefetch)
        {
            var messageId = reader.IsDBNull(0) 
                ? Guid.Empty 
                : reader.GetGuid(0);
            var streamVersion = reader.GetInt32(1);
            var position = reader.GetInt64(2);
            var createdUtc = reader.GetDateTime(3);
            var type = reader.GetString(4);
            var jsonMetadata = reader.IsDBNull(5) ? default : reader.GetString(5);

            return new StreamMessage(
                    idInfo.SqlStreamId.IdOriginal,
                    messageId,
                    streamVersion,
                    position,
                    createdUtc,
                    type,
                    jsonMetadata,
                    ct => prefetch 
                        ? new Task<String>(() => (reader.IsDBNull(6)) 
                                ? default
                                : reader.GetTextReader(6).ReadToEnd())
                        : GetJsonData(idInfo.SqlStreamId.Id, streamVersion));
        }
    }
}