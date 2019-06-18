namespace SqlStreamStore.V1.Internal.HoneyBearHalClient
{
    using System.Collections.Generic;
    using System.Threading;
    using System.Threading.Tasks;

    internal static class HalClientPostExtensions
    {
        public static Task<IHalClient> Post(
            this IHalClient client, 
            string rel, 
            object value, 
            object parameters, 
            string curie,
            IDictionary<string, string[]> headers = null,
            CancellationToken cancellationToken = default)
        {
            headers = headers ?? new Dictionary<string, string[]>();
            
            var relationship = HalClientExtensions.Relationship(rel, curie);

            return client.BuildAndExecuteAsync(relationship, parameters, 
                uri => client.Client.PostAsync(uri, value, headers, cancellationToken));
        }
    }
}
