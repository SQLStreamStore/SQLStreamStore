namespace SqlStreamStore
{
    using System;
    using SqlStreamStore.Imports.Ensure.That;
    using SqlStreamStore.Infrastructure;
    using SqlStreamStore.Subscriptions;

    public class PostgresStreamStoreSettings
    {
        private string _schema = "public";

        public PostgresStreamStoreSettings(string connectionString)
        {
            Ensure.That(connectionString, nameof(connectionString)).IsNotNullOrWhiteSpace();

            ConnectionString = connectionString;
        }

        public string ConnectionString { get; private set; }

        public CreateStreamStoreNotifier CreateStreamStoreNotifier { get; set; } =
            store => new PollingStreamStoreNotifier(store);

        public string Schema
        {
            get { return _schema; }
            set
            {
                if (string.IsNullOrWhiteSpace(value))
                {
                    Ensure.That(value, nameof(Schema)).IsNotNullOrWhiteSpace();
                }
                _schema = value;
            }
        }

        public TimeSpan MetadataMaxAgeCacheExpire { get; set; } = TimeSpan.FromMinutes(1);

        public int MetadataMaxAgeCacheMaxSize { get; set; } = 10000;

        public GetUtcNow GetUtcNow { get; set; }

        public string LogName { get; set; } = "PostgresStreamStore";

        public JsonBSupport JsonB { get; set; } = JsonBSupport.AutoDetect;
    }

    public enum JsonBSupport
    {
        AutoDetect,
        Disable
    }
}