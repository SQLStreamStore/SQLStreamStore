namespace SqlStreamStore.Internal.HoneyBearHalClient
{
    using System.Linq;
    using System.Threading.Tasks;
    using SqlStreamStore.Internal.HoneyBearHalClient.Models;

    internal static class HalClientGetExtensions
    {
        public static Task<IHalClient> GetAsync(this IHalClient client, IResource resource, string rel) =>
            client.GetAsync(resource, rel, null, null);

        public static Task<IHalClient> GetAsync(
            this IHalClient client,
            IResource resource,
            string rel,
            object parameters) =>
            client.GetAsync(resource, rel, parameters, null);

        public static async Task<IHalClient> GetAsync(
            this IHalClient client,
            IResource resource,
            string rel,
            object parameters,
            string curie)
        {
            var relationship = HalClientExtensions.Relationship(rel, curie);

            if(resource.Embedded.Any(e => e.Rel == relationship))
            {
                var current = resource.Embedded.Where(e => e.Rel == relationship);

                return new HalClient(client, current);
            }

            var link = resource.Links.FirstOrDefault(l => l.Rel == relationship);
            if(link == null)
                throw new FailedToResolveRelationship(relationship);

            return await client.ExecuteAsync(
                HalClientExtensions.Construct(resource.BaseAddress, link, parameters),
                uri => client.Client.GetAsync(uri));
        }
    }
}