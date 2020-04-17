[assembly: System.Runtime.CompilerServices.InternalsVisibleTo("SqlStreamStore.Sqlite.Tests")]

namespace SqlStreamStore
{
    using System;
    using System.Collections.Concurrent;
    using System.Collections.Generic;
    using System.Linq;
    using System.Reflection;
    using System.Threading;
    using System.Threading.Tasks;
    using Microsoft.Data.Sqlite;
    using SqlStreamStore.Infrastructure;
    using SqlStreamStore.Logging;
    using SqlStreamStore.Streams;
    using SqlStreamStore.Subscriptions;

    public partial class SqliteStreamStore : StreamStoreBase
    {
        private static readonly System.Collections.Concurrent.ConcurrentDictionary<string, string> _scripts = new ConcurrentDictionary<string, string>();
        private static readonly Assembly s_assembly = typeof(SqliteStreamStore)
            .GetTypeInfo()
            .Assembly;
        
        private readonly Lazy<IStreamStoreNotifier> _streamStoreNotifier;
        private readonly SqliteStreamStoreSettings _settings;
        private readonly Func<IEnumerable<StreamMessage>, ReadDirection, int, int> _resolveNextVersion;

        public SqliteStreamStore(SqliteStreamStoreSettings settings)
            : base(settings.MetadataMaxAgeCacheExpire,
                settings.MetadataMaxAgeCacheMaxSize,
                settings.GetUtcNow,
                settings.LogName)
        {
            _settings = settings;
            
            _streamStoreNotifier = new Lazy<IStreamStoreNotifier>(() =>
            {
                if(_settings.CreateStreamStoreNotifier == null)
                {
                    throw new InvalidOperationException(
                        "Cannot create notifier because supplied createStreamStoreNotifier was null");
                }

                return settings.CreateStreamStoreNotifier.Invoke(this);
            });
            
            _resolveNextVersion = (messages, direction, currentVersion) =>
            {
                if(messages.Any())
                {
                    var mVers = messages.Last().StreamVersion;
                    mVers = direction == ReadDirection.Forward
                        ? mVers + 1
                        : mVers < StreamVersion.Start ? StreamVersion.Start : mVers - 1;

                    return mVers;
                }

                currentVersion = direction == ReadDirection.Forward
                    ? currentVersion + 1
                    : currentVersion == StreamVersion.End ? StreamVersion.End : currentVersion - 1;
                
                return currentVersion;
            };
        }

        public SqliteStreamStore(Infrastructure.GetUtcNow getUtcNow, string logName) : base(getUtcNow, logName)
        {
            throw new NotSupportedException();
        }

        protected override async Task<long> ReadHeadPositionInternal(CancellationToken cancellationToken)
        {
            GuardAgainstDisposed();
            cancellationToken.ThrowIfCancellationRequested();

            using(var connection = OpenConnection())
            {
                return (await connection
                    .AllStream()
                    .ReadHeadPosition(cancellationToken)) ?? Position.End;
            }
        }

        internal void CreateSchemaIfNotExists()
        {
            using(var connection = OpenConnection(false))
            using(var command = connection.CreateCommand())
            {
                command.CommandText = GetScript("Tables");
                command.ExecuteNonQuery();
            }
        }

        internal SqliteConnection OpenConnection(bool isReadOnly = true)
        {
            var connection = new SqliteConnection(_settings.GetConnectionString(isReadOnly));
            connection.Open();
            using(var command = connection.CreateCommand())
            {
                command.CommandText = @"PRAGMA foreign_keys = true;
                                        PRAGMA journal_mode = WAL;";
                command.ExecuteNonQuery();
            }
            
            // register functions.
            connection.CreateFunction("split", (string source) => source.Split(new[]{','}, StringSplitOptions.RemoveEmptyEntries));
            connection.CreateFunction("contains",
                (string allIdsBeingSought, string idToSeek) =>
                {
                    var ids = allIdsBeingSought.Split(new[] { ',' }, StringSplitOptions.RemoveEmptyEntries);
                    return ids.Contains(idToSeek, StringComparer.OrdinalIgnoreCase);
                });
            return connection;
        }

        internal async Task TryScavengeAsync(string streamId, CancellationToken ct)
        {
            //RESEARCH: Is this valid?
            if(streamId.Equals(SqliteStreamId.Deleted.Id) || streamId.StartsWith("$"))
            {
                return;
            }

            var deletedMessageIds = new List<Guid>();
            using(var connection = OpenConnection(false))
            using(var command = connection.CreateCommand())
            {
                long idInternal = 0;
                long? maxCount = default;
                
                command.CommandText = @"SELECT streams.id_internal, streams.max_count 
                                        FROM streams 
                                        WHERE streams.id_original = @streamId";
                command.Parameters.Clear();
                command.Parameters.AddWithValue("@streamId", streamId);
                using(var reader = command.ExecuteReader())
                {
                    if(reader.Read())
                    {
                        idInternal = reader.ReadScalar<long>(0, -1);
                        maxCount = reader.ReadScalar<long?>(1);
                    }
                }

                command.CommandText = @"SELECT COUNT(*)
                                        FROM messages
                                        WHERE messages.stream_id_internal = @idInternal;";
                command.Parameters.Clear();
                command.Parameters.AddWithValue("@idInternal", idInternal);
                var streamLength = command.ExecuteScalar<long>(StreamVersion.Start);

                var limit = streamLength - maxCount;
                if(limit > 0)
                {
                    // capture all messages that should be removed.
                    command.CommandText = @"SELECT messages.event_id
                                            FROM messages
                                            WHERE messages.stream_id_internal = @idInternal
                                            ORDER BY messages.stream_version ASC 
                                            LIMIT @limit";
                    command.Parameters.Clear();
                    command.Parameters.AddWithValue("@idInternal", idInternal);
                    command.Parameters.AddWithValue("@limit", limit);

                    using(var reader = command.ExecuteReader())
                    {
                        while(reader.Read())
                        {
                            deletedMessageIds.Add(reader.ReadScalar(0, Guid.Empty));
                        }
                    }
                }
            }

            Logger.Info(
                "Found {deletedMessageIdCount} message(s) for stream {streamId} to scavenge.",
                deletedMessageIds.Count,
                streamId);

            if(deletedMessageIds.Count > 0)
            {
                Logger.Debug(
                    "Scavenging the following messages on stream {streamId}: {deletedMessageIds}",
                    streamId,
                    deletedMessageIds);

                foreach(var deletedMessageId in deletedMessageIds)
                {
                    await DeleteEventInternal(streamId, deletedMessageId, ct).NotOnCapturedContext();
                }
            }
        }
        
        private string GetScript(string name) => _scripts.GetOrAdd(name,
            key =>
            {
                var resourceNames = s_assembly.GetManifestResourceNames();
                var resourceStreamName = $"{typeof(SqliteStreamStore).Namespace}.Scripts.{key}.sql";
                using(var stream = s_assembly.GetManifestResourceStream(resourceStreamName))
                {
                    if(stream == null)
                    {
                        throw new System.Exception($"Embedded resource, {name}, not found. BUG!");
                    }

                    using(System.IO.StreamReader reader = new System.IO.StreamReader(stream))
                    {
                        return reader
                            .ReadToEnd();
                    }
                }
            });

        private int? ResolveInternalStreamId(string streamId, SqliteCommand cmd = null, bool throwIfNotExists = true)
        {
            int? PerformDelete(SqliteCommand command)
            {
                command.CommandText = "SELECT id_internal FROM streams WHERE id_original = @streamId";
                command.Parameters.Clear();
                command.Parameters.AddWithValue("@streamId", streamId);
                var result = command.ExecuteScalar<int?>();
                
                if(throwIfNotExists && result == null)
                {
                    throw new Exception("Stream does not exist.");
                }

                return result;
            }

            if(cmd != null)
                return PerformDelete(cmd);

            using (var conn = OpenConnection(false))
            using(var command = conn.CreateCommand())
            {
                return PerformDelete(command);
            }
        }
    }
}