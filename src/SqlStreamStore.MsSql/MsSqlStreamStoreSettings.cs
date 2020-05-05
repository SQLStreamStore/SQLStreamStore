namespace SqlStreamStore
{
    using System;
    using Microsoft.Data.SqlClient;
    using SqlStreamStore.Imports.Ensure.That;
    using SqlStreamStore.Infrastructure;
    using SqlStreamStore.Subscriptions;

    /// <summary>
    ///     Represents setting to configure a <see cref="MsSqlStreamStore"/>
    /// </summary>
    [Obsolete("Use MsSqlStreamStoreV3Settings instead. Note: this will require a schema and data migration.", false)]
    public class MsSqlStreamStoreSettings
    {
        private string _schema = "dbo";
        private int _commandTimeout = 30;

        /// <summary>
        ///     Initialized a new instance of <see cref="MsSqlStreamStoreSettings"/>.
        /// </summary>
        /// <param name="connectionString"></param>
        public MsSqlStreamStoreSettings(string connectionString)
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
        ///     creates <see cref="PollingStreamStoreNotifier"/>
        /// </summary>
        public CreateStreamStoreNotifier CreateStreamStoreNotifier { get; set; } =
            store => new PollingStreamStoreNotifier(store);

        /// <summary>
        ///     MsSqlStream store supports stores in a single database through 
        ///     the useage of schema. This is useful if you want to contain
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
        public Func<string, SqlConnection> ConnectionFactory { get; set; } = connectionString => new SqlConnection(connectionString);

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