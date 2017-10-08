namespace SqlStreamStore.HalClient
{
    using System.Collections.Generic;
    using System.Threading;
    using System.Threading.Tasks;
    using SqlStreamStore.HalClient.Models;

    /// <summary>
    /// Extension methods implementing HTTP POST operations
    /// </summary>
    internal static class HalClientPostExtensions
    {
        /// <summary>
        /// Makes a HTTP POST request to the given link relation on the most recently navigated resource.
        /// </summary>
        /// <param name="client">The instance of the client used for the request.</param>
        /// <param name="rel">The link relation to follow.</param>
        /// <param name="value">The payload to POST.</param>
        /// <returns>A new instance of <see cref="IHalClient"/> with updated resources.</returns>
        /// <exception cref="FailedToResolveRelationship" />
        public static Task<IHalClient> Post(this IHalClient client, string rel, object value) => 
            client.Post(rel, value, null, null);

        /// <summary>
        /// Makes a HTTP POST request to the given link relation on the most recently navigated resource.
        /// </summary>
        /// <param name="client">The instance of the client used for the request.</param>
        /// <param name="rel">The link relation to follow.</param>
        /// <param name="value">The payload to POST.</param>
        /// <param name="curie">The curie of the link relation.</param>
        /// <returns>A new instance of <see cref="IHalClient"/> with updated resources.</returns>
        /// <exception cref="FailedToResolveRelationship" />
        public static Task<IHalClient> Post(this IHalClient client, string rel, object value, string curie) => 
            client.Post(rel, value, null, curie);

        /// <summary>
        /// Makes a HTTP POST request to the given templated link relation on the most recently navigated resource.
        /// </summary>
        /// <param name="client">The instance of the client used for the request.</param>
        /// <param name="rel">The templated link relation to follow.</param>
        /// <param name="value">The payload to POST.</param>
        /// <param name="parameters">An anonymous object containing the template parameters to apply.</param>
        /// <returns>A new instance of <see cref="IHalClient"/> with updated resources.</returns>
        /// <exception cref="FailedToResolveRelationship" />
        /// <exception cref="TemplateParametersAreRequired" />
        public static Task<IHalClient> Post(this IHalClient client, string rel, object value, object parameters) => 
            client.Post(rel, value, parameters, null);

        /// <summary>
        /// Makes a HTTP POST request to the given templated link relation on the most recently navigated resource.
        /// </summary>
        /// <param name="rel">The templated link relation to follow.</param>
        /// <param name="value">The payload to POST.</param>
        /// <param name="parameters">An anonymous object containing the template parameters to apply.</param>
        /// <param name="curie">The curie of the link relation.</param>
        /// <returns>A new instance of <see cref="IHalClient"/> with updated resources.</returns>
        /// <param name="client">The instance of the client used for the request.</param>
        /// <exception cref="FailedToResolveRelationship" />
        /// <exception cref="TemplateParametersAreRequired" />
        public static Task<IHalClient> Post(
            this IHalClient client, 
            string rel, 
            object value, 
            object parameters, 
            string curie,
            IDictionary<string, string[]> headers = null,
            CancellationToken cancellationToken = default(CancellationToken))
        {
            headers = headers ?? new Dictionary<string, string[]>();
            
            var relationship = HalClientExtensions.Relationship(rel, curie);

            return client.BuildAndExecuteAsync(relationship, parameters, 
                uri => client.Client.PostAsync(uri, value, headers, cancellationToken));
        }

    }
}
