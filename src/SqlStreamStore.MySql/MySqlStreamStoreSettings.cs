namespace SqlStreamStore
{
    using System;
    using MySql.Data.MySqlClient;
    using SqlStreamStore.Imports.Ensure.That;
    using SqlStreamStore.Infrastructure;
    using SqlStreamStore.Subscriptions;

    public class MySqlStreamStoreSettings
    {
        private Func<string, MySqlConnection> _connectionFactory;
        private GetUtcNow _getUtcNow = SystemClock.GetUtcNow;
        private int _appendDeadlockRetryAttempts = 0;
        private readonly MySqlConnectionStringBuilder _connectionStringBuilder;

        /// <summary>
        /// Initializes a new instance of <see cref="MySqlStreamStoreSettings"/>.
        /// </summary>
        /// <param name="connectionString">The connection string.</param>
        public MySqlStreamStoreSettings(string connectionString)
        {
            Ensure.That(connectionString, nameof(connectionString)).IsNotNullOrWhiteSpace();

            _connectionStringBuilder = new MySqlConnectionStringBuilder(connectionString)
            {
                GuidFormat = MySqlGuidFormat.Binary16,
                ConnectionReset = true,
                UseCompression = true
            };
        }

        /// <summary>
        ///     Gets the connection string.
        /// </summary>
        public string ConnectionString => _connectionStringBuilder.ConnectionString;

        /// <summary>
        ///     Allows overriding of the stream store notifier. The default implementation
        ///     creates <see cref="PollingStreamStoreNotifier"/>.
        /// </summary>
        public CreateStreamStoreNotifier CreateStreamStoreNotifier { get; set; } =
            store => new PollingStreamStoreNotifier(store);

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
        ///     The log name used for any of the log messages.
        /// </summary>
        public string LogName { get; } = nameof(MySqlStreamStore);

        /// <summary>
        ///     Allows overriding the way a <see cref="MySqlConnection"/> is created given a connection string.
        ///     The default implementation simply passes the connection string into the <see cref="MySqlConnection"/> constructor.
        /// </summary>
        public Func<string, MySqlConnection> ConnectionFactory
        {
            get => _connectionFactory ?? (_connectionFactory = connectionString => new MySqlConnection(connectionString));
            set
            {
                Ensure.That(value, nameof(value)).IsNotNull();
                _connectionFactory = value;
            }
        }

        /// <summary>
        ///     Disables stream and message deletion tracking. Will increase
        ///     performance, however subscribers won't know if a stream or a
        ///     message has been deleted. This can be modified at runtime.
        /// </summary>
        public bool DisableDeletionTracking { get; set; }

        /// <summary>
        ///     Indicates how many times an append operation should be retried
        ///     if a deadlock is detected. Defaults to 0.
        /// </summary>
        public int AppendDeadlockRetryAttempts
        {
            get => _appendDeadlockRetryAttempts;
            set
            {
                Ensure.That(value, nameof(value)).IsGte(0);
                _appendDeadlockRetryAttempts = value;
            }
        }
    }
}