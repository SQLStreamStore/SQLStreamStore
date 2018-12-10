namespace SqlStreamStore
{
    using System;
    using Npgsql;
    using SqlStreamStore.Imports.Ensure.That;
    using SqlStreamStore.Infrastructure;
    using SqlStreamStore.Subscriptions;

    public class PostgresStreamStoreSettings
    {
        private string _schema = "public";
        private Func<string, NpgsqlConnection> _connectionFactory;

        /// <summary>
        /// Initializes a new instance of <see cref="PostgresStreamStoreSettings"/>.
        /// </summary>
        /// <param name="connectionString">The connection string.</param>
        public PostgresStreamStoreSettings(string connectionString)
        {
            Ensure.That(connectionString, nameof(connectionString)).IsNotNullOrWhiteSpace();

            ConnectionString = connectionString;
            CreateStreamStoreNotifier = _ => new PostgresListenNotifyStreamStoreNotifier(this);
        }

        /// <summary>
        ///     Gets the connection string.
        /// </summary>
        public string ConnectionString { get; }

        /// <summary>
        ///     Allows overriding of the stream store notifier. The default implementation
        ///     creates <see cref="PollingStreamStoreNotifier"/>.
        /// </summary>
        public CreateStreamStoreNotifier CreateStreamStoreNotifier { get; set; }

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
        public GetUtcNow GetUtcNow { get; set; }

        /// <summary>
        ///     The log name used for any of the log messages.
        /// </summary>
        public string LogName { get; set; } = nameof(PostgresStreamStore);

        /// <summary>
        ///     Allows setting whether or not deleting expired (i.e., older than maxCount) messages happens in the same database transaction as append to stream or not.
        ///     This does not effect scavenging when setting a stream's metadata - it is always run in the same transaction.
        /// </summary>
        public bool ScavengeAsynchronously { get; set; } = true;

        /// <summary>
        ///     Allows overriding the way a <see cref="NpgsqlConnection"/> is created given a connection string.
        ///     The default implementation simply passes the connection string into the <see cref="NpgsqlConnection"/> constructor.
        /// </summary>
        public Func<string, NpgsqlConnection> ConnectionFactory
        {
            get => _connectionFactory 
                   ?? (_connectionFactory = connectionString => new NpgsqlConnection(connectionString));
            set
            {
                Ensure.That(value, nameof(value)).IsNotNull();
                _connectionFactory = value;
            }
        }
    }
}