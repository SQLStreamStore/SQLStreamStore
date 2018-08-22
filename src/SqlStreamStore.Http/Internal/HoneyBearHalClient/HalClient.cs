namespace SqlStreamStore.Internal.HoneyBearHalClient
{
    using System.Collections.Generic;
    using System.Linq;
    using System.Net;
    using System.Net.Http;
    using Newtonsoft.Json;
    using SqlStreamStore.Internal.HoneyBearHalClient.Http;
    using SqlStreamStore.Internal.HoneyBearHalClient.Models;

    internal class HalClient : IHalClient
    {
        public IJsonHttpClient Client { get; }

        public IEnumerable<IResource> Current { get; } = Enumerable.Empty<IResource>();
        
        public HttpStatusCode? StatusCode { get; }

        public HalClient(HttpClient client, JsonSerializer serializer)
        {
            Client = new JsonHttpClient(client, serializer);
        }

        /// <summary>
        /// Creates a copy of the specified client with given resources.
        /// </summary>
        /// <param name="client">The client to copy.</param>
        /// <param name="current">The new resources.</param>
        public HalClient(IHalClient client, IEnumerable<IResource> current)
        {
            Client = client.Client;
            Current = current;
            StatusCode = client.StatusCode;
        }

        public HalClient(IHalClient client, IEnumerable<IResource> current, HttpStatusCode statusCode)
        {
            Client = client.Client;
            Current = current;
            StatusCode = statusCode;
        }
    }
}
