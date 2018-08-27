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
        ///     A delegate to return the current UTC now. Used in testing to
        ///     control timestamps and time related operations.
        /// </summary>
        public GetUtcNow GetUtcNow { get; set; } = SystemClock.GetUtcNow;

        /// <summary>
        ///     The log name used for the any log messages.
        /// </summary>
        public string LogName { get; set; } = nameof(HttpClientSqlStreamStore);

    }
}