namespace SqlStreamStore
{
    using System;
    using EnsureThat;
    using SqlStreamStore.Infrastructure;
    using SqlStreamStore.Subscriptions;

    /// <summary>
    ///     Represents setting to configure a <see cref="MsSqlStreamStore"/>
    /// </summary>
    public class MsSqlStreamStoreSettings
    {
        private string _schema = "dbo";

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
        ///     Allows overrding of the stream store notifier. The default implementation
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
        ///     To help with perf, the max age of messages in a stream,
        ///     are cached. It is not expected that a streams max age
        ///     metadata to be changed frequently. Here we hold on to the
        ///     max age for the specified timespan. The default is 1 minute.
        /// </summary>
        public TimeSpan MetadataMaxAgeCacheExpire { get; set; } = TimeSpan.FromMinutes(1);


        /// <summary>
        ///     To help with perf, the max age of messages in a stream,
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
    }
}