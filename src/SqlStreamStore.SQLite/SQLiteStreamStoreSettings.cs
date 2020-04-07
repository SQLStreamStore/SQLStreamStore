namespace SqlStreamStore
{
    using System;
    using Microsoft.Data.Sqlite;
    using SqlStreamStore.Imports.Ensure.That;
    using SqlStreamStore.Infrastructure;
    using SqlStreamStore.Subscriptions;

    public class SQLiteStreamStoreSettings
    {
        private string _schema = "public";
        private Func<string, SqliteConnection> _connectionFactory;
        private GetUtcNow _getUtcNow = SystemClock.GetUtcNow;

        /// <summary>
        /// Initializes a new instance of <see cref="SQLiteStreamStoreSettings"/>.
        /// </summary>
        /// <param name="connectionString">The connection string.</param>
        public SQLiteStreamStoreSettings(string connectionString)
        {
            Ensure.That(connectionString, nameof(connectionString)).IsNotNullOrWhiteSpace();

            ConnectionString = connectionString;
        }

        /// <summary>
        ///     Gets the connection string.
        /// </summary>
        public string ConnectionString { get; }

        /// <summary>
        ///     Allows overriding of the stream store notifier. The default implementation
        ///     creates <see cref="PollingStreamStoreNotifier"/>.
        /// </summary>
        public CreateStreamStoreNotifier CreateStreamStoreNotifier { get; set; } =
            store => new PollingStreamStoreNotifier(store);

        /// <summary>
        ///     The schema SQL Stream Store should place database objects into. Defaults to "public".
        /// </summary>
        public string Schema
        {
            get => _schema;
            set
            {
                Ensure.That(value, nameof(Schema)).IsNotNullOrWhiteSpace();

                _schema = value;
            }
        }

        /// <summary>
        ///     Loads the auto_explain module and turns it on for all queries. Useful for index tuning.
        /// </summary>
        public bool ExplainAnalyze { get; set; }

        /// <summary>
        ///     A delegate to return the current UTC now. Used in testing to
        ///     control timestamps and time related operations.
        /// </summary>
        public GetUtcNow GetUtcNow
        {
            get => _getUtcNow;
            set => _getUtcNow = value ?? SystemClock.GetUtcNow;
        }

        /// <summary>
        ///     To help with perf, the max age of messages in a stream
        ///     are cached. It is not expected that a streams max age
        ///     metadata to be changed frequently. Here we hold on to the
        ///     max age for the specified timespan. The default is 1 minute.
        /// </summary>
        public TimeSpan MetadataMaxAgeCacheExpire { get; set; } = TimeSpan.FromMinutes(1);

        /// <summary>
        ///     To help with perf, the max age of messages in a stream
        ///     are cached. It is not expected that a streams max age
        ///     metadata to be changed frequently. Here we define how many
        ///     items are cached. The default value is 10000.
        /// </summary>
        public int MetadataMaxAgeCacheMaxSize { get; set; } = 10000;

        /// <summary>
        ///     The log name used for any of the log messages.
        /// </summary>
        public string LogName { get; } = nameof(SQLiteStreamStore);

        /// <summary>
        ///     Allows overriding the way a <see cref="SqliteConnection"/> is created given a connection string.
        ///     The default implementation simply passes the connection string into the <see cref="SqliteConnection"/> constructor.
        /// </summary>
        public virtual Func<string, SqliteConnection> ConnectionFactory
        {
            get => _connectionFactory 
                   ?? (_connectionFactory = connectionString => new SqliteConnection(connectionString));
            
            
            set
            {
                Ensure.That(value, nameof(value)).IsNotNull();
                _connectionFactory = value;
            }
        }
    }
}