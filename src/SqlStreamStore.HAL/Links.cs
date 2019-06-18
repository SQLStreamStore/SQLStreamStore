namespace SqlStreamStore
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Net.Http;
    using Halcyon.HAL;
    using Microsoft.AspNetCore.Http;
    using SqlStreamStore.Logging;

    internal class Links
    {
        private static readonly ILog s_log = LogProvider.For<Links>();
        
        private readonly PathString _path;
        private readonly List<(string rel, string href, string title)> _links;
        private readonly string _relativePathToRoot;

        public static Links FromOperation<T>(IStreamStoreOperation<T> operation) => FromPath(operation.Path);
        
        public static Links FromPath(PathString path) => new Links(path);

        public static Links FromRequestMessage(HttpRequestMessage requestMessage)
            => FromPath(new PathString(requestMessage.RequestUri.AbsolutePath));
        
        private Links(PathString path)
        {
            _path = path;
            var segmentCount = path.Value.Split('/').Length;
            _relativePathToRoot = 
                segmentCount < 2
                    ? "./"
                    : string.Join(string.Empty, Enumerable.Repeat("../", segmentCount - 2));

            _links = new List<(string rel, string href, string title)>();
        }

        public Links Add(string rel, string href, string title = null)
        {
            if(rel == null)
                throw new ArgumentNullException(nameof(rel));
            if(href == null)
                throw new ArgumentNullException(nameof(href));
            
            _links.Add((rel, href, title));

            s_log.Debug("Added link {link} to response for request {path}", _links[_links.Count - 1], _path);
            return this;
        }

        public Links Self()
        {
            var (_, href, title) = _links[_links.Count - 1];

            return Add(Constants.Relations.Self, href, title);
        }

        public Links AddSelf(string rel, string href, string title = null)
            => Add(rel, href, title)
                .Add(Constants.Relations.Self, href, title);

        public Link[] ToHalLinks()
        {
            var links = new Link[_links.Count + 1];

            for(var i = 0; i < _links.Count; i++)
            {
                var (rel, href, title) = _links[i];

                links[i] = new Link(rel, Resolve(href), title, replaceParameters: false)
                {
                    Type = Constants.MediaTypes.HalJson
                };
            }

            links[_links.Count] = new Link(
                Constants.Relations.Curies,
                Resolve(LinkFormatter.DocsTemplate()),
                "Documentation",
                replaceParameters: false)
            {
                Name = Constants.Relations.StreamStorePrefix,
                HrefLang = "en",
                Type = Constants.MediaTypes.TextMarkdown
            };

            return links;
        }

        private string Resolve(string relativeUrl) => $"{_relativePathToRoot}{relativeUrl}";

        public static implicit operator Link[](Links links) => links.ToHalLinks();

        private static string FormatLink(
            string baseAddress,
            string direction,
            int maxCount,
            long position,
            bool prefetch)
            => $"{baseAddress}?d={direction}&m={maxCount}&p={position}&e={(prefetch ? 1 : 0)}";

        [Obsolete("", true)]
        public static string FormatBackwardLink(string baseAddress, int maxCount, long position, bool prefetch)
            => FormatLink(baseAddress, "b", maxCount, position, prefetch);
    }
}