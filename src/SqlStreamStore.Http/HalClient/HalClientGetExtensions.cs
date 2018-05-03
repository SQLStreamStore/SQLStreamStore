namespace SqlStreamStore.HalClient
{
    using System.Linq;
    using System.Threading.Tasks;
    using SqlStreamStore.HalClient.Models;

    /// <summary>
    /// Extension methods implementing HTTP GET operations
    /// </summary>
    internal static class HalClientGetExtensions
    {
        /// <summary>
        /// Navigates the given link relation and stores the the returned resource(s).
        /// </summary>
        /// <param name="client">The instance of the client used for the request.</param>
        /// <param name="resource">The current <see cref="IResource"/>.</param>
        /// <param name="rel">The link relation to follow.</param>
        /// <returns>A new instance of <see cref="IHalClient"/> with updated resources.</returns>
        /// <exception cref="FailedToResolveRelationship" />
        public static Task<IHalClient> GetAsync(this IHalClient client, IResource resource, string rel) =>
            client.GetAsync(resource, rel, null, null);

        /// <summary>
        /// Navigates the given templated link relation and stores the the returned resource(s).
        /// </summary>
        /// <param name="client">The instance of the client used for the request.</param>
        /// <param name="resource">The current <see cref="IResource"/>.</param>
        /// <param name="rel">The templated link relation to follow.</param>
        /// <param name="parameters">An anonymous object containing the template parameters to apply.</param>
        /// <returns>A new instance of <see cref="IHalClient"/> with updated resources.</returns>
        /// <exception cref="FailedToResolveRelationship" />
        /// <exception cref="TemplateParametersAreRequired" />
        public static Task<IHalClient> GetAsync(this IHalClient client, IResource resource, string rel, object parameters) =>
            client.GetAsync(resource, rel, parameters, null);

        /// <summary>
        /// Navigates the given templated link relation and stores the the returned resource(s).
        /// </summary>
        /// <param name="client">The instance of the client used for the request.</param>
        /// <param name="resource">The current <see cref="IResource"/>.</param>
        /// <param name="rel">The templated link relation to follow.</param>
        /// <param name="parameters">An anonymous object containing the template parameters to apply.</param>
        /// <param name="curie">The curie of the link relation.</param>
        /// <returns>A new instance of <see cref="IHalClient"/> with updated resources.</returns>
        /// <exception cref="FailedToResolveRelationship" />
        /// <exception cref="TemplateParametersAreRequired" />
        public static async Task<IHalClient> GetAsync(this IHalClient client, IResource resource, string rel, object parameters, string curie)
        {
            var relationship = HalClientExtensions.Relationship(rel, curie);

            if (resource.Embedded.Any(e => e.Rel == relationship))
            {
                var current = resource.Embedded.Where(e => e.Rel == relationship);
              
                return new HalClient(client, current);
            }

            var link = resource.Links.FirstOrDefault(l => l.Rel == relationship);
            if (link == null)
                throw new FailedToResolveRelationship(relationship);
            
            return await client.ExecuteAsync(
                HalClientExtensions.Construct(resource.BaseAddress, link, parameters), 
                uri => client.Client.GetAsync(uri));
        }
    }
}
