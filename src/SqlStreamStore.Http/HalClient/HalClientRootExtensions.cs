namespace SqlStreamStore.HalClient
{
    using System.Linq;
    using System.Threading.Tasks;

    /// <summary>
    /// A lightweight fluent .NET client for navigating and consuming HAL APIs.
    /// </summary>
    internal static class HalClientRootExtensions
    {
        /// <summary>
        /// Makes a HTTP GET request to the default URI and stores the returned resource.
        /// </summary>
        /// <returns>A new instance of <see cref="IHalClient"/> with updated resources.</returns>
        public static Task<IHalClient> RootAsync(this IHalClientBase client) =>
            client.ExecuteAsync(string.Empty, uri => client.Client.GetAsync(uri));
        
        /// <summary>
        /// Makes a HTTP GET request to the given URL and stores the returned resource.
        /// </summary>
        /// <param name="client">The instance of the client used for the request.</param>
        /// <param name="href">The URI to request.</param>
        /// <returns>A new instance of <see cref="IHalClient"/> with updated resources.</returns>
        public static Task<IHalClient> RootAsync(this IHalClientBase client, string href) =>
            client.ExecuteAsync(href, uri => client.Client.GetAsync(uri));

        /// <summary>
        /// Determines whether the most recently navigated resource contains the given link relation.
        /// </summary>
        /// <param name="client">The instance of the client which recenctly navigated rources are checked.</param>
        /// <param name="rel">The link relation to look for.</param>
        /// <returns>Whether or not the link relation exists.</returns>
        public static bool Has(this IHalClient client, string rel) =>
            client.Has(rel, null);

        /// <summary>
        /// Determines whether the most recently navigated resource contains the given link relation.
        /// </summary>
        /// <param name="client">The instance of the client which recenctly navigated rources are checked.</param>
        /// <param name="rel">The link relation to look for.</param>
        /// <param name="curie">The curie of the link relation.</param>
        /// <returns>Whether or not the link relation exists.</returns>
        public static bool Has(this IHalClient client, string rel, string curie)
        {
            var relationship = HalClientExtensions.Relationship(rel, curie);

            return
                client.Current.Any(r => r.Embedded.Any(e => e.Rel == relationship))
                || client.Current.Any(r => r.Links.Any(l => l.Rel == relationship));
        }
    }
}
