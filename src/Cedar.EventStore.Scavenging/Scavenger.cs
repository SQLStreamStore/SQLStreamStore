namespace Cedar.EventStore.Scavenging
{
    using System;
    using System.Data.SQLite;
    using System.Diagnostics;
    using System.IO;
    using System.Linq;
    using System.Threading;
    using System.Threading.Tasks;
    using Cedar.EventStore.Streams;

    public class Scavenger
    {
        public const int SqliteSchemaVersion = 1;
        private const string DbName = "scavenger.db";
        private readonly DirectoryInfo _dbDir;
        private readonly string _dbPath;
        private readonly IEventStore _eventStore;
        private ConcurrentExclusiveSchedulerPair _scheduler;
        private Task<IAllStreamSubscription> _subscribeToAll;

        public Scavenger(IEventStore eventStore, DirectoryInfo dbDir)
            : this(eventStore, dbDir, SqliteSchemaVersion)
        {}

        // Only used in tests
        internal Scavenger(IEventStore eventStore, DirectoryInfo dbDir, int schemaVersionOverride)
        {
            _eventStore = eventStore;
            _dbDir = dbDir;
            _dbPath = Path.Combine(_dbDir.FullName, DbName);
            Initialize(schemaVersionOverride);
        }

        public Task Completion => _scheduler.Completion;

        public void Complete()
        {
            _subscribeToAll?.Dispose();
            _scheduler.Complete();
        }

        public event EventHandler<StreamEvent> StreamEventHandled;

        public Task<int> GetSchemaVersion()
        {
            return StartNewTask(GetSchemaVersionInternal);
        }

        public Task<int?> GetCheckpoint()
        {
            return StartNewTask(GetCheckpointInternal);
        }

        private void Initialize(int schemaVersion)
        {
            if(!_dbDir.Exists)
            {
                _dbDir.Create();
            }
            if(_dbDir.GetFiles(DbName).SingleOrDefault() == null)
            {
                WriteEmbeddedSqliteDbFile();
            }
            else
            {
                if(GetSchemaVersionInternal() != schemaVersion)
                {
                    File.Delete(Path.Combine(_dbDir.FullName, DbName));
                    WriteEmbeddedSqliteDbFile();
                }
            }

            _scheduler = new ConcurrentExclusiveSchedulerPair(TaskScheduler.Default, 1);

            var checkpoint = GetCheckpointInternal();
            _subscribeToAll = _eventStore.SubscribeToAll(
                checkpoint,
                StreamEventProcessed,
                (reason, exception) => { });
        }

        private Task StreamEventProcessed(StreamEvent streamEvent)
        {
            return StartNewTask(() =>
            {
                HandleStreamEvent(streamEvent);
                RaiseStreamEventProcessed(streamEvent);
            });
        }

        private void HandleStreamEvent(StreamEvent streamEvent)
        {
            if(!streamEvent.StreamId.StartsWith("$"))
            {
                
            }
        }

        private SQLiteConnection CreateConnection()
        {
            return new SQLiteConnection($"Data Source={_dbPath};Version=3;");
        }

        private void RaiseStreamEventProcessed(StreamEvent streamEvent)
        {
            Volatile.Read(ref StreamEventHandled)?.Invoke(this, streamEvent);
        }

        private int GetSchemaVersionInternal()
        {
            using(var connection = CreateConnection())
            {
                connection.Open();
                using(var sqLiteCommand = connection.CreateCommand())
                {
                    sqLiteCommand.CommandText = Scripts.GetVersion;
                    return int.Parse((string) sqLiteCommand.ExecuteScalar());
                }
            }
        }

        private int? GetCheckpointInternal()
        {
            using(var connection = CreateConnection())
            {
                connection.Open();
                using(var command = connection.CreateCommand())
                {
                    command.CommandText = Scripts.GetCheckpoint;
                    var result = command.ExecuteScalar() as string;
                    if(result == null)
                    {
                        return null;
                    }
                    return int.Parse(result);
                }
            }
        }

        private Task StartNewTask(Action action)
        {
            return Task.Factory.StartNew(
                action,
                CancellationToken.None,
                TaskCreationOptions.None,
                _scheduler.ConcurrentScheduler);
        }

        private Task<T> StartNewTask<T>(Func<T> func)
        {
            return Task.Factory.StartNew(
                func,
                CancellationToken.None,
                TaskCreationOptions.None,
                _scheduler.ConcurrentScheduler);
        }

        private void SaveCheckpoint(long checkpoint)
        {
            using(var connection = CreateConnection())
            {
                using(var command = connection.CreateCommand())
                {
                    command.CommandText = Scripts.SaveCheckpoint;
                    command.Parameters.AddWithValue("checkpoint", checkpoint);
                    command.ExecuteNonQuery();
                }
            }
        }

        private void WriteEmbeddedSqliteDbFile()
        {
            var assembly = typeof(Scavenger).Assembly;
            var resourceName = assembly
                .GetManifestResourceNames()
                .Single(name => name.EndsWith(DbName));
            using(var file = File.OpenWrite(Path.Combine(_dbDir.FullName, DbName)))
            {
                using(var stream = assembly.GetManifestResourceStream(resourceName))
                {
                    Debug.Assert(stream != null, "stream != null");
                    stream.CopyTo(file);
                }
            }
        }

        private static class Scripts
        {
            internal static readonly string GetCheckpoint = "SELECT Value FROM Meta WHERE Key = 'Checkpoint'";
            internal static readonly string GetVersion = "SELECT Value FROM Meta WHERE Key = 'Version'";

            internal static readonly string SaveCheckpoint =
                "UDATE Meta SET Value = '@checkpoint' WHERE Key = 'Checkpoint'";
        }
    }

    internal class StreamMetadata
    {
        public string StreamId { get; set; }

        public int MaxAge { get; set; }
    }

    internal class StreamEventMaxAge
    {
        public string StreamId { get; set; }

        public string EventId { get; set; }

        public DateTime Created { get; set; }

        public DateTime Expires { get; set; }
    }
}