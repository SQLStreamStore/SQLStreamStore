namespace SqlStreamStore.HalClient
{
    using System.Net;
    using SqlStreamStore.HalClient.Http;

    /// <summary>
    /// A lightweight fluent .NET client for navigating and consuming HAL APIs without state.
    /// </summary>
    internal interface IHalClientBase
    {
        /// <summary>
        /// Gets the instance of the implementation of <see cref="IJsonHttpClient"/> used by the <see cref="HalClient"/>.
        /// </summary>
        IJsonHttpClient Client { get; }
        
        HttpStatusCode? StatusCode { get; }
    }
}