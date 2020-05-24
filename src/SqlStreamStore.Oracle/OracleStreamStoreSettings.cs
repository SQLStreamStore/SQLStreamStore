namespace SqlStreamStore.Oracle
{
    using System;
    using global::Oracle.ManagedDataAccess.Client;
    using SqlStreamStore.Imports.Ensure.That;
    using SqlStreamStore.Infrastructure;
    using SqlStreamStore.Subscriptions;

    public class OracleStreamStoreSettings
    {
        private string _schema;
        private int _commandTimeout = 30;

        /// <summary>
        ///     Initialized a new instance of <see cref="OracleStreamStoreSettings"/>.
        /// </summary>
        /// <param name="schema"></param>
        /// <param name="connectionString"></param>
        public OracleStreamStoreSettings(string schema, string connectionString)
        {
            Ensure.That(connectionString, nameof(connectionString)).IsNotNullOrWhiteSpace();
            Ensure.That(schema, nameof(schema)).IsNotNullOrWhiteSpace();

            ConnectionString = connectionString;
            Schema = schema;
        }

        /// <summary>
        ///     Gets the connection string.
        /// </summary>
        public string ConnectionString { get; }

        /// <summary>
        ///     Allows overriding of the stream store notifier. The default implementation
        ///     creates <see cref="PollingStreamStoreNotifier"/>
        /// </summary>
        public CreateStreamStoreNotifier CreateStreamStoreNotifier { get; set; } =
            store => new PollingStreamStoreNotifier(store);

        /// <summary>
        ///     MsSqlStream store supports stores in a single database through 
        ///     the usage of schema. This is useful if you want to contain
        ///     multiple bounded contexts in a single database. Alternative is
        ///     use a database per bounded context, which may be more appropriate
        ///     for larger stores.
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
        ///     A delegate to return the current UTC now. Used in testing to
        ///     control timestamps and time related operations.
        /// </summary>
        public GetUtcNow GetUtcNow { get; set; }

        /// <summary>
        ///     The log name used for the any log messages.
        /// </summary>
        public string LogName { get; set; } = "MsSqlStreamStore";

        /// <summary>
        ///     Allows overriding the way a <see cref="SqlConnection"/> is created given a connection string.
        ///     The default implementation simply passes the connection string into the <see cref="SqlConnection"/> constructor.
        /// </summary>
        public Func<string, OracleConnection> ConnectionFactory { get; set; } = connectionString => new OracleConnection(connectionString);

        /// <summary>
        ///     Disables stream and message deletion tracking. Will increase
        ///     performance, however subscribers won't know if a stream or a
        ///     message has been deleted. This can be modified at runtime.
        /// </summary>
        public bool DisableDeletionTracking { get; set; }

        /// <summary>
        ///     Controls the wait time to execute a command. Defaults to 30 seconds.
        /// </summary>
        public int CommandTimeout
        {
            get => _commandTimeout;
            set
            {
                Ensure.That(value, nameof(CommandTimeout)).IsGte(0);
                _commandTimeout = value;
            }
        }
    }
}