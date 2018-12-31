namespace SqlStreamStore.HAL.Docs
{
    using System;
    using System.Linq;

    internal class DocsResource : IResource
    {
        private readonly IResource[] _resources;
        public SchemaSet Schema { get; }

        public DocsResource(params IResource[] resources)
        {
            if(resources == null)
            {
                throw new ArgumentException(nameof(resources));
            }

            _resources = resources;
        }

        public Response Get(string rel) => (from resource in _resources
                let documentationStream = resource.Schema?.GetDocumentation(rel)
                where documentationStream != null
                select new MarkdownResponse(documentationStream))
            .FirstOrDefault();
    }
}