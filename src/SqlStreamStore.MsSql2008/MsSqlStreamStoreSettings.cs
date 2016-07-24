namespace SqlStreamStore
{
    using System;
    using EnsureThat;
    using SqlStreamStore.Infrastructure;
    using SqlStreamStore.Subscriptions;

    public class MsSqlStreamStoreSettings
    {
        private string _schema = "dbo";

        public MsSqlStreamStoreSettings(string connectionString)
        {
            Ensure.That(connectionString, nameof(connectionString)).IsNotNullOrWhiteSpace();

            ConnectionString = connectionString;
        }

        public string ConnectionString { get; private set; }

        public CreateEventStoreNotifier CreateEventStoreNotifier { get; set; } =
            PollingEventStoreNotifier.CreateEventStoreNotifier();

        public string Schema
        {
            get {  return _schema; }
            set
            {
                if(string.IsNullOrWhiteSpace(value))
                {
                    Ensure.That(value, nameof(Schema)).IsNotNullOrWhiteSpace();
                }
                _schema = value;
            }
        }

        public TimeSpan MetadataMaxAgeCacheExpire { get; set; } = TimeSpan.FromMinutes(1);

        public int MetadataMaxAgeCacheMaxSize { get; set; } = 10000;

        public GetUtcNow GetUtcNow { get; set; }

        public string LogName { get; set; } = "MsSqlStreamStore";
    }
}