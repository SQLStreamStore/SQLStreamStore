namespace SqlStreamStore.V1.Docs
{
    using System;
    using System.Linq;
    using System.Net.Http.Headers;
    using Microsoft.Extensions.Primitives;

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

        public Response Get(string rel, StringValues acceptHeader) => (from resource in _resources
                from accept in acceptHeader
                let mediaTypeWithQuality = ParseAcceptHeader(accept)
                where mediaTypeWithQuality != null
                orderby mediaTypeWithQuality.Quality ?? 1.0 descending
                let documentationStream = resource.Schema?.GetDocumentation(rel, mediaTypeWithQuality.MediaType)
                where documentationStream != null
                select new MarkdownResponse(documentationStream))
            .FirstOrDefault();

        private static MediaTypeWithQualityHeaderValue ParseAcceptHeader(string accept)
            => MediaTypeWithQualityHeaderValue.TryParse(accept, out var mediaTypeWithQuality)
                ? mediaTypeWithQuality
                : null;
    }
}