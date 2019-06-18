namespace SqlStreamStore.V1
{
    using System;
    using System.Net.Http;
    using Microsoft.Extensions.DependencyInjection;
    using SqlStreamStore.V1.Infrastructure;
    using SqlStreamStore.V1.Subscriptions;

    public class HttpClientSqlStreamStoreSettings
    {
        private static readonly Lazy<Func<HttpClient>> s_defaultHttpClientFactory = new Lazy<Func<HttpClient>>(() =>
        {
            var serviceProvider = new ServiceCollection()
                .AddHttpClient(nameof(HttpClientSqlStreamStore)).Services
                .BuildServiceProvider();

            return serviceProvider.GetService<IHttpClientFactory>().CreateClient;
        }); 
        /// <summary>
        /// The root uri of the server. Must end with "/".
        /// </summary>
        public Uri BaseAddress { get; set; }

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

        /// <summary>
        /// The HttpClient Factory
        /// </summary>
        public Func<HttpClient> CreateHttpClient { get; set; } = s_defaultHttpClientFactory.Value;
    }
}