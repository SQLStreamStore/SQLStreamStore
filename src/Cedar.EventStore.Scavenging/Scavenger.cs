namespace Cedar.EventStore.Scavenging
{
    using System;
    using System.Data.SQLite;
    using System.IO;
    using System.Linq;

    public class Scavenger : IDisposable
    {
        private SQLiteConnection _connection;

        public Scavenger(IEventStore eventStore, string path)
        {
            string dbPath = Path.Combine(path, "scavenger.db");
            if(!Directory.Exists(path))
            {
                Directory.CreateDirectory(path);
            }
            if(!File.Exists(dbPath))
            {
                var assembly = typeof(Scavenger).Assembly;
                var resourceName = assembly
                    .GetManifestResourceNames()
                    .Single(name => name.EndsWith("scavenger.sqlite"));
                using(var file = File.OpenWrite(dbPath))
                {
                    using(Stream stream = assembly.GetManifestResourceStream(resourceName))
                    {
                        stream.CopyTo(file);
                    }
                }
            }

            _connection = new SQLiteConnection($"Data Source={dbPath};Version=3;");
        }

        public void Blah()
        {
            
        }

        public void Dispose()
        {
            _connection?.Dispose();
            _connection = null;
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
