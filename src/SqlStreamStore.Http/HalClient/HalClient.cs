namespace SqlStreamStore.HalClient
{
    using System.Collections.Generic;
    using System.Linq;
    using System.Net;
    using System.Net.Http;
    using SqlStreamStore.HalClient.Http;
    using SqlStreamStore.HalClient.Models;

    /// <summary>
    /// A lightweight fluent .NET client for navigating and consuming HAL APIs.
    /// </summary>
    internal class HalClient : IHalClient
    {
        /// <summary>
        /// Gets the instance of the implementation of <see cref="IJsonHttpClient"/> used by the <see cref="HalClient"/>.
        /// </summary>
        public IJsonHttpClient Client { get; }

        /// <summary>
        /// The most recently navigated resource.
        /// </summary>
        public IEnumerable<IResource> Current { get; } = Enumerable.Empty<IResource>();
        
        public HttpStatusCode? StatusCode { get; }


        /// <summary>
        /// Creates an instance of the <see cref="HoneyBear.HalClient"/> class.
        /// </summary>
        /// <param name="client">The <see cref="System.Net.Http.HttpClient"/> to use.</param>
        public HalClient(
            HttpClient client)
        {
            Client = new JsonHttpClient(client);
        }


        /// <summary>
        /// Creates an instance of the <see cref="HoneyBear.HalClient"/> class.
        /// Uses a default instance of <see cref="System.Net.Http.HttpClient"/>.
        /// </summary>
        public HalClient()
            : this(new HttpClient())
        {

        }

        /// <summary>
        /// Creates an instance of the <see cref="HoneyBear.HalClient"/> class.
        /// </summary>
        /// <param name="client">The implementation of <see cref="IJsonHttpClient"/> to use.</param>
        public HalClient(
            IJsonHttpClient client)
        {
            Client = client;
        }

        /// <summary>
        /// Creates a copy of the specified client with given resources.
        /// </summary>
        /// <param name="client">The client to copy.</param>
        /// <param name="current">The new resources.</param>
        public HalClient(IHalClientBase client, IEnumerable<IResource> current)
        {
            Client = client.Client;
            Current = current;
            StatusCode = client.StatusCode;
        }

        /// <summary>
        /// Creates a copy of the specified client with given resources.
        /// </summary>
        /// <param name="client">The client to copy.</param>
        /// <param name="current">The new resources.</param>
        public HalClient(IHalClientBase client, IEnumerable<IResource> current, HttpStatusCode statusCode)
        {
            Client = client.Client;
            Current = current;
            StatusCode = statusCode;
        }
    }
}
