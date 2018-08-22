namespace SqlStreamStore.Internal.HoneyBearHalClient
{
    using System.Collections.Generic;
    using System.Threading;
    using System.Threading.Tasks;

    internal static class HalClientDeleteExtensions
    {
        public static Task<IHalClient> Delete(
            this IHalClient client,
            string rel,
            object parameters,
            string curie,
            IDictionary<string, string[]> headers = null,
            CancellationToken cancellationToken = default)
        {
            var relationship = HalClientExtensions.Relationship(rel, curie);

            return client.BuildAndExecuteAsync(
                relationship,
                parameters,
                uri => client.Client.DeleteAsync(uri, headers, cancellationToken));
        }
    }
}