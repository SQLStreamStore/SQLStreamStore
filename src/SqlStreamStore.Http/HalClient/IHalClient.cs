namespace SqlStreamStore.HalClient
{
    using System.Collections.Generic;
    using System.Net;
    using SqlStreamStore.HalClient.Http;
    using SqlStreamStore.HalClient.Models;

    /// <summary>
    /// A lightweight fluent .NET client for navigating and consuming HAL APIs containing the current state.
    /// </summary>
    internal interface IHalClient
    {
        /// <summary>
        /// The most recently navigated resource.
        /// </summary>
        /// 
        IEnumerable<IResource> Current { get; }

        /// <summary>
        /// Gets the instance of the implementation of <see cref="IJsonHttpClient"/> used by the <see cref="HalClient"/>.
        /// </summary>
        IJsonHttpClient Client { get; }

        HttpStatusCode? StatusCode { get; }
    }
}