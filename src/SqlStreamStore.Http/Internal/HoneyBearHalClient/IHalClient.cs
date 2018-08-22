namespace SqlStreamStore.Internal.HoneyBearHalClient
{
    using System.Collections.Generic;
    using System.Net;
    using SqlStreamStore.Internal.HoneyBearHalClient.Http;
    using SqlStreamStore.Internal.HoneyBearHalClient.Models;

    internal interface IHalClient
    {
        IEnumerable<IResource> Current { get; }

        IJsonHttpClient Client { get; }

        HttpStatusCode? StatusCode { get; }
    }
}