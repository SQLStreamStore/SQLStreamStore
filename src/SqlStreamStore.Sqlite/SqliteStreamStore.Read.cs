namespace SqlStreamStore
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
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
            var maxRecords = count == int.MaxValue ? count - 1 : count;
            var streamVersion = start == StreamVersion.End ? int.MaxValue : start;
            using (var connection = OpenConnection())
            using (var command = connection.CreateCommand())
            {
                var idInfo = new StreamIdInfo(streamId);
                int streamIdInternal = 0;
                int lastStreamVersion = 0;
                long lastStreamPosition = 0;
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
                    lastStreamPosition = reader.GetInt64(2);
                    maxAge = reader.ReadScalar(3, default(int?));
                    var streamsMaxRecords = reader.ReadScalar(4, StreamVersion.End);
                    maxRecords = Math.Min(maxRecords, (streamsMaxRecords == StreamVersion.End ? maxRecords : streamsMaxRecords));
                }

                command.CommandText = @"SELECT messages.position
                                        FROM messages
                                        WHERE messages.stream_id_internal = @streamIdInternal
                                            AND messages.stream_version = @streamVersion;";
                command.Parameters.Clear();
                command.Parameters.AddWithValue("@streamIdInternal", streamIdInternal);
                command.Parameters.AddWithValue("@streamVersion", streamVersion);
                var position = command.ExecuteScalar<long?>();

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
                        WHERE messages.stream_id_internal = @idInternal AND messages.stream_version >= @streamVersion; 
                        
                        SELECT messages.event_id,
                               messages.stream_version,
                               messages.[position],
                               messages.created_utc,
                               messages.type,
                               messages.json_metadata,
                               case when @prefetch then messages.json_data else null end as json_data
                        FROM messages
                        WHERE messages.stream_id_internal = @idInternal AND messages.[stream_version] >= @streamVersion
                        ORDER BY messages.position
                        LIMIT @count;";
                command.Parameters.Clear();
                command.Parameters.AddWithValue("@idInternal", streamIdInternal);
                command.Parameters.AddWithValue("@streamVersion", lastStreamVersion);
                command.Parameters.AddWithValue("@prefetch", prefetch);
                command.Parameters.AddWithValue("@count", maxRecords + 1);

                var messages = new List<(StreamMessage message, int? maxAge)>();
                using(var reader = command.ExecuteReader())
                {
                    reader.Read();
                    var remainingMessages = Convert.ToInt32(reader.GetValue(0));

                    reader.NextResult();
                    var _continue = true;
                    while(reader.Read() && _continue)
                    {
                        if(messages.Count == count)
                        {
                            _continue = false;
                            continue;
                        }
                        
                        messages.Add((ReadStreamMessage(reader, idInfo, prefetch), maxAge));
                        lastStreamVersion = reader.ReadScalar(1, StreamVersion.End);
                    }
                    if(_continue && reader.Read())
                    {
                        lastStreamVersion = reader.ReadScalar(1, StreamVersion.End);
                    }
                    
                    var filtered = FilterExpired(messages);
                    var isEnd = remainingMessages - messages.Count <= maxRecords;
                    var nextVersion = messages.Skip(1).Any() ? messages.Last().message.StreamVersion + 1 : StreamVersion.Start; 
                    
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
            int fromStreamVersion,
            int count,
            bool prefetch,
            ReadNextStreamPage readNext,
            CancellationToken cancellationToken)
        {
            GuardAgainstDisposed();
            cancellationToken.ThrowIfCancellationRequested();

            // If the count is int.MaxValue, TSql will see it as a negative number. 
            // Users shouldn't be using int.MaxValue in the first place anyway.
            var maxRecords = count == int.MaxValue ? count - 1 : count;
            var streamVersion =  fromStreamVersion == StreamVersion.End ? int.MaxValue -1 : fromStreamVersion;
            using (var connection = OpenConnection())
            using (var command = connection.CreateCommand())
            {
                var idInfo = new StreamIdInfo(streamId);
                int streamIdInternal = 0;
                int startStreamVersion = streamVersion;
                int lastStreamVersion = 0;
                long lastStreamPosition = 0;
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
                            startStreamVersion,
                            StreamVersion.End,
                            StreamVersion.End,
                            StreamVersion.End,
                            ReadDirection.Backward,
                            true,
                            readNext));
                    }

                    streamIdInternal = reader.GetInt32(0);
                    lastStreamVersion = reader.GetInt32(1);
                    lastStreamPosition = reader.GetInt32(2);
                    maxAge = reader.ReadScalar(3, default(int?));
                    maxRecords = Math.Min(maxRecords, reader.ReadScalar(4, 0));
                }

                command.CommandText = @"SELECT messages.position
                                        FROM messages
                                        WHERE messages.stream_id_internal = @streamIdInternal
                                            AND messages.stream_version = @streamVersion
                                        --ORDER BY messages.position DESC
                                        LIMIT 1;";
                command.Parameters.Clear();
                command.Parameters.AddWithValue("@streamIdInternal", streamIdInternal);
                command.Parameters.AddWithValue("@streamVersion", streamVersion);
                var position = command.ExecuteScalar<long?>();

                if(position == null)
                {
                    command.CommandText = @"SELECT MAX(messages.position)
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
                        WHERE messages.stream_id_internal = @idInternal AND messages.[position] <= @position;
                         
                        SELECT messages.event_id,
                               messages.stream_version,
                               messages.[position],
                               messages.created_utc,
                               messages.[type],
                               messages.json_metadata,
                               case when @prefetch then messages.json_data else null end as json_data
                        FROM messages
                        WHERE messages.stream_id_internal = @idInternal AND messages.[position] <= @position
                        ORDER BY messages.position DESC
                        LIMIT @count;";
                command.Parameters.AddWithValue("@idInternal", streamIdInternal);
                command.Parameters.AddWithValue("@position", position);
                command.Parameters.AddWithValue("@prefetch", prefetch);
                command.Parameters.AddWithValue("@count", maxRecords + 1);

                var messages = new List<(StreamMessage message, int? maxAge)>();
                using(var reader = command.ExecuteReader())
                {
                    reader.Read();
                    var remainingMessages = Convert.ToInt32(reader.GetValue(0));

                    reader.NextResult();
                    var _continue = true;
                    while(reader.Read() && _continue)
                    {
                        if(messages.Count == count)
                        {
                            lastStreamVersion = reader.ReadScalar(1, StreamVersion.End);
                            lastStreamPosition = reader.ReadScalar(2, Position.End);
                            _continue = false;
                            continue;
                        }
                        
                        messages.Add((ReadStreamMessage(reader, idInfo, prefetch), maxAge));
                        lastStreamVersion = reader.ReadScalar(1, StreamVersion.End);
                        lastStreamPosition = reader.ReadScalar(2, Position.End);
                    }

                    var filtered = FilterExpired(messages);
                    var isEnd = remainingMessages - messages.Count <= maxRecords;
                    var nextVersion = messages.Skip(1).Any() ? messages.Last().message.StreamVersion - 1 : lastStreamVersion; 
                    
                    var page = new ReadStreamPage(
                        streamId,
                        PageReadStatus.Success,
                        fromStreamVersion,
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
            var jsonData = prefetch 
                ? !reader.IsDBNull(6) 
                    ? reader.GetTextReader(6).ReadToEnd() 
                    : string.Empty
                : string.Empty;

            return new StreamMessage(
                idInfo.SqlStreamId.IdOriginal,
                messageId,
                streamVersion,
                position,
                createdUtc,
                type,
                jsonMetadata,
                ct => prefetch
                    ? Task.FromResult(jsonData)
                    : GetJsonData(idInfo.SqlStreamId.Id, streamVersion)
            );
        }
    }
}