namespace SqlStreamStore.V1.Internal.HoneyBearHalClient
{
    using System.Collections.Generic;
    using System.Net;
    using SqlStreamStore.V1.Internal.HoneyBearHalClient.Http;
    using SqlStreamStore.V1.Internal.HoneyBearHalClient.Models;

    internal interface IHalClient
    {
        IEnumerable<IResource> Current { get; }

        IJsonHttpClient Client { get; }

        HttpStatusCode? StatusCode { get; }
    }
}