namespace SqlStreamStore
{
    using System;
    using EnsureThat;
    using SqlStreamStore.Infrastructure;
    using SqlStreamStore.Subscriptions;

    /// <summary>
    ///     Contains settings to configure an <see cref="MsSqlStreamStore"/> instance.
    /// </summary>
    public class MsSqlStreamStoreSettings
    {
        /// <summary>
        ///     The default database schema that contains the stream store tables - 'dbo'.
        /// </summary>
        public const string DefaultSchema = "dbo";

        private string _schema = DefaultSchema;

        /// <summary>
        ///     Initializes a new 
        /// </summary>
        /// <param name="connectionString"></param>
        public MsSqlStreamStoreSettings(string connectionString)
        {
            Ensure.That(connectionString, nameof(connectionString)).IsNotNullOrWhiteSpace();

            ConnectionString = connectionString;
        }

        /// <summary>
        ///     Connection string the MS SQL Database instance.
        /// </summary>
        public string ConnectionString { get; private set; }

        /// <summary>
        ///     A delegate to create a stream store notifier. Defauls is a <see cref="PollingStreamStoreNotifier"/>.
        /// </summary>
        public CreateStreamStoreNotifier CreateStreamStoreNotifier { get; set; } =
            store => new PollingStreamStoreNotifier(store);


        /// <summary>
        ///     The database schema to use for the stream store tables.
        /// </summary>
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

        /// <summary>
        ///     To help with performance, an internal cache of stream metadata Max Age values are retained. This
        ///     property sets the expiration time span of cached items.
        /// </summary>
        public TimeSpan MetadataMaxAgeCacheExpire { get; set; } = TimeSpan.FromMinutes(1);

        /// <summary>
        ///      To help with performance, an internal cache of stream metadata Max Age values are retained. This
        ///      property sets the maxiumum number of items to retain. Default is 10000.
        /// </summary>
        public int MetadataMaxAgeCacheMaxSize { get; set; } = 10000;

        /// <summary>
        ///     A delegate to get the current UTC date time.
        /// </summary>
        public GetUtcNow GetUtcNow { get; set; }

        /// <summary>
        ///     The logger name 
        /// </summary>
        public string LogName { get; set; } = "MsSqlStreamStore";
    }
}