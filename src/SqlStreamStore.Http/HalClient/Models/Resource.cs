namespace SqlStreamStore.HalClient.Models
{
    using System.Collections.Generic;

    internal sealed class Resource : Dictionary<string, object>, IResource
    {
        public Resource()
        {
            Links = new List<ILink>();
            Embedded = new List<IResource>();
        }

        public string Rel { get; set; }
        public string Href { get; set; }
        public string Name { get; set; }
        public IList<ILink> Links { get; set; }
        public IList<IResource> Embedded { get; set; }
    }

    internal sealed class Resource<T> : Dictionary<string, object>, IResource<T>
        where T : class, new()
    {
        public Resource()
        {
            Links = new List<ILink>();
            Embedded = new List<IResource>();
            Data = new T();
        }

        public string Rel { get; set; }
        public string Href { get; set; }
        public string Name { get; set; }
        public IList<ILink> Links { get; set; }
        public IList<IResource> Embedded { get; set; }
        public T Data { get; set; }
    }
}