namespace SqlStreamStore
{
    using System;
    using System.Net.Http;
    using SqlStreamStore.Infrastructure;
    using SqlStreamStore.Subscriptions;

    public class HttpClientSqlStreamStoreSettings
    {
        /// <summary>
        /// The root uri of the server. Must end with "/".
        /// </summary>
        public Uri BaseAddress { get; set; }

        /// <summary>
        ///     The Http Message Handler. Override this for testing.
        /// </summary>
        public HttpMessageHandler HttpMessageHandler { get; set; } = new HttpClientHandler();

        public CreateStreamStoreNotifier CreateStreamStoreNotifier { get; set; } =
            store => new PollingStreamStoreNotifier(store);

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
        public string LogName { get; set; } = nameof(HttpClientSqlStreamStore);

    }
}