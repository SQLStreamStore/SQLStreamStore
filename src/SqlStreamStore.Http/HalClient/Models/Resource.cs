namespace SqlStreamStore.HalClient.Models
{
    using System;
    using System.Collections.Generic;
    using System.Linq;

    internal sealed class Resource : Dictionary<string, object>, IResource
    {
        public Resource()
            : base(StringComparer.OrdinalIgnoreCase)
        {
            Links = new List<ILink>();
            Embedded = new List<IResource>();
        }

        private Resource(IDictionary<string, object> other, IEqualityComparer<string> comparer)
            : base(other, comparer)
        {
            Links = new List<ILink>();
            Embedded = new List<IResource>();
        }

        public string Rel { get; set; }
        public string Href { get; set; }
        public string Name { get; set; }
        public IList<ILink> Links { get; set; }
        public IList<IResource> Embedded { get; set; }
        public Uri BaseAddress { get; private set; }

        public IResource WithBaseAddress(Uri baseAddress)
            => new Resource(this, Comparer)
            {
                Links = Links,
                BaseAddress = baseAddress,
                Embedded = Embedded.Select(r => r.WithBaseAddress(baseAddress)).ToArray(),
                Name = Name,
                Rel = Rel
            };

        public override string ToString() =>
            $"Rel={Rel},Href={Href},Name={Name},Links={string.Join(", ", Links)},Embedded={string.Join(", ", Embedded)}";
    }
}