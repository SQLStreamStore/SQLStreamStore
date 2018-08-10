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

        public string ConnectionString { get; }

        public CreateStreamStoreNotifier CreateStreamStoreNotifier { get; set; } =
            store => new PollingStreamStoreNotifier(store);

        public string Schema
        {
            get => _schema;
            set
            {
                Ensure.That(value, nameof(Schema)).IsNotNullOrWhiteSpace();

                _schema = value;
            }
        }

        public bool ExplainAnalyze { get; set; }

        public GetUtcNow GetUtcNow { get; set; }

        public string LogName { get; set; } = nameof(PostgresStreamStore);

        public bool ScavengeAsynchronously { get; set; } = true;
    }
}