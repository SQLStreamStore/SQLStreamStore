namespace SqlStreamStore.V1.Internal.HoneyBearHalClient
{
    using System.Linq;
    using System.Threading;
    using System.Threading.Tasks;

    internal static class HalClientRootExtensions
    {
        public static Task<IHalClient> RootAsync(
            this IHalClient client,
            string href,
            CancellationToken cancellationToken = default) =>
            client.ExecuteAsync(href, uri => client.Client.GetAsync(uri, cancellationToken));

        public static bool Has(this IHalClient client, string rel) =>
            client.Has(rel, null);

        public static bool Has(this IHalClient client, string rel, string curie)
        {
            var relationship = HalClientExtensions.Relationship(rel, curie);

            return
                client.Current.Any(r => r.Embedded.Any(e => e.Rel == relationship))
                || client.Current.Any(r => r.Links.Any(l => l.Rel == relationship));
        }
    }
}