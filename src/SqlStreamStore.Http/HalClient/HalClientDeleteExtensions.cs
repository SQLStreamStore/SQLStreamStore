namespace SqlStreamStore.HalClient
{
    using System.Threading.Tasks;
    using SqlStreamStore.HalClient.Models;

    /// <summary>
    /// Extension methods implementing HTTP DELETE operations
    /// </summary>
    internal static class HalClientDeleteExtensions
    {
        /// <summary>
        /// Makes a HTTP DELETE request to the given link relation on the most recently navigated resource.
        /// </summary>
        /// <param name="client">The instance of the client used for the request.</param>
        /// <param name="rel">The link relation to follow.</param>
        /// <returns>A new instance of <see cref="IHalClient"/> with updated resources.</returns>
        /// <exception cref="FailedToResolveRelationship" />
        public static Task<IHalClient> Delete(this IHalClient client, string rel) => 
            client.Delete(rel, null, null);

        /// <summary>
        /// Makes a HTTP DELETE request to the given link relation on the most recently navigated resource.
        /// </summary>
        /// <param name="client">The instance of the client used for the request.</param>
        /// <param name="rel">The link relation to follow.</param>
        /// <param name="curie">The curie of the link relation.</param>
        /// <returns>A new instance of <see cref="IHalClient"/> with updated resources.</returns>
        /// <exception cref="FailedToResolveRelationship" />
        public static Task<IHalClient> Delete(this IHalClient client, string rel, string curie) => 
            client.Delete(rel, null, curie);

        /// <summary>
        /// Makes a HTTP DELETE request to the given templated link relation on the most recently navigated resource.
        /// </summary>
        /// <param name="client">The instance of the client used for the request.</param>
        /// <param name="rel">The templated link relation to follow.</param>
        /// <param name="parameters">An anonymous object containing the template parameters to apply.</param>
        /// <returns>A new instance of <see cref="IHalClient"/> with updated resources.</returns>
        /// <exception cref="FailedToResolveRelationship" />
        /// <exception cref="TemplateParametersAreRequired" />
        public static Task<IHalClient> Delete(this IHalClient client, string rel, object parameters) => 
            client.Delete(rel, parameters, null);

        /// <summary>
        /// Makes a HTTP DELETE request to the given templated link relation on the most recently navigated resource.
        /// </summary>
        /// <param name="client">The instance of the client used for the request.</param>
        /// <param name="rel">The templated link relation to follow.</param>
        /// <param name="parameters">An anonymous object containing the template parameters to apply.</param>
        /// <param name="curie">The curie of the link relation.</param>
        /// <returns>A new instance of <see cref="IHalClient"/> with updated resources.</returns>
        /// <exception cref="FailedToResolveRelationship" />
        /// <exception cref="TemplateParametersAreRequired" />
        public static Task<IHalClient> Delete(this IHalClient client, string rel, object parameters, string curie)
        {
            var relationship = HalClientExtensions.Relationship(rel, curie);

            return client.BuildAndExecuteAsync(relationship, parameters, uri => client.Client.DeleteAsync(uri));
        }
    }
}
