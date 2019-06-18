namespace SqlStreamStore
{
    using System.Collections.Generic;
    using System.Linq;

    internal class Resource
    {
        public Resource(dynamic state, Link[] links, (string rel, Resource resource)[] embedded)
        {
            State = state;
            Links = links.GroupBy(l => l.Rel).ToDictionary(g => g.Key, g => g.ToArray());
            Embedded = embedded.GroupBy(e => e.rel).ToDictionary(g => g.Key, g => g.Select(e => e.resource).ToArray());
        }

        public dynamic State { get; }
        public IReadOnlyDictionary<string, Link[]> Links { get; }
        public IReadOnlyDictionary<string, Resource[]> Embedded { get; }
    }
}