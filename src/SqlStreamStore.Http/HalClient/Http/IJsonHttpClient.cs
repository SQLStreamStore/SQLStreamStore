namespace SqlStreamStore.HalClient.Http
{
    using System.Collections.Generic;
    using System.Net.Http;
    using System.Threading;
    using System.Threading.Tasks;

    /// <summary>
    /// Provides a wrapper for <see cref="System.Net.Http.HttpClient"/> that processes JSON HTTP requests and responses.
    /// </summary>
    internal interface IJsonHttpClient
    {
        /// <summary>
        /// A getter for the wrapped instance of <see cref="System.Net.Http.HttpClient"/>.
        /// </summary>
        HttpClient HttpClient { get; }

        /// <summary>
        /// Send a GET request to the specified Uri as an asynchronous operation.
        /// </summary>
        /// <param name="uri">The Uri the request is sent to.</param>
        /// <returns>Returns <see cref="T:System.Threading.Tasks.Task`1"/>.The task object representing the asynchronous operation.</returns>
        Task<HttpResponseMessage> GetAsync(string uri, CancellationToken cancellationToken = default(CancellationToken));

        /// <summary>
        /// Send a HEAD request to the specified Uri as an asynchronous operation.
        /// </summary>
        /// <param name="uri">The Uri the request is sent to.</param>
        /// <returns>Returns <see cref="T:System.Threading.Tasks.Task`1"/>.The task object representing the asynchronous operation.</returns>
        Task<HttpResponseMessage> HeadAsync(string uri, CancellationToken cancellationToken = default(CancellationToken));

        /// <summary>
        /// Send an OPTIONS request to the specified Uri as an asynchronous operation.
        /// </summary>
        /// <param name="uri">The Uri the request is sent to.</param>
        /// <returns>Returns <see cref="T:System.Threading.Tasks.Task`1"/>.The task object representing the asynchronous operation.</returns>
        Task<HttpResponseMessage> OptionsAsync(string uri, CancellationToken cancellationToken = default(CancellationToken));

        /// <summary>
        /// Send a POST request to the specified Uri as an asynchronous operation.
        /// </summary>
        /// <typeparam name="T"/>
        /// <param name="uri">The Uri the request is sent to.</param>
        /// <param name="value">The HTTP request content sent to the server.</param>
        /// <returns>Returns <see cref="T:System.Threading.Tasks.Task`1"/>.The task object representing the asynchronous operation.</returns>
        Task<HttpResponseMessage> PostAsync<T>(string uri, T value, IDictionary<string, string[]> headers, CancellationToken cancellationToken = default(CancellationToken));

        /// <summary>
        /// Send a DELETE request to the specified Uri as an asynchronous operation.
        /// </summary>
        /// <param name="uri">The Uri the request is sent to.</param>
        /// <returns>Returns <see cref="T:System.Threading.Tasks.Task`1"/>.The task object representing the asynchronous operation.</returns>
        Task<HttpResponseMessage> DeleteAsync(string uri, CancellationToken cancellationToken = default(CancellationToken));
    }
}
