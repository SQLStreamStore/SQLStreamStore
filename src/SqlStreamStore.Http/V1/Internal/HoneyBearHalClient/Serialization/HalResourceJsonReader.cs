namespace SqlStreamStore.V1.Internal.HoneyBearHalClient.Serialization
{
    using System;
    using System.Collections;
    using System.Collections.Generic;
    using System.Linq;
    using System.Threading;
    using System.Threading.Tasks;
    using Newtonsoft.Json;
    using Newtonsoft.Json.Linq;
    using SqlStreamStore.V1.Internal.HoneyBearHalClient.Models;

    internal static class HalResourceJsonReader
    {
        public static async Task<IResource> ReadResource(
            JsonReader reader,
            CancellationToken cancellationToken = default)
            => new JResource(await JObject.LoadAsync(reader, cancellationToken));

        private class JResource : IResource
        {
            private readonly JObject _inner;

            public string Rel { get; set; }

            public string Href
            {
                get => _inner.Value<string>("href");
                set => _inner["href"] = value;
            }

            public string Name
            {
                get => _inner.Value<string>("name");
                set => _inner["name"] = value;
            }

            public JResource(JObject inner, string rel = null)
            {
                if(inner == null)
                    throw new ArgumentNullException(nameof(inner));
                _inner = inner;
                Rel = rel;
            }

            public IEnumerator<KeyValuePair<string, object>> GetEnumerator()
                => _inner.Properties()
                    .Select(x => new KeyValuePair<string, object>(
                        x.Name,
                        x.Value))
                    .GetEnumerator();

            IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();

            public IList<ILink> Links => GetLinks().ToArray();
            public IList<IResource> Embedded => GetEmbedded().ToArray();

            public Uri BaseAddress { get; private set; }

            public IResource WithBaseAddress(Uri baseAddress)
                => new JResource(_inner, Rel)
                {
                    BaseAddress = baseAddress
                };

            private IEnumerable<ILink> GetLinks()
            {
                var links = _inner.Value<JObject>("_links");

                foreach(var property in links.Properties())
                {
                    if(property.Value.Type == JTokenType.Array)
                    {
                        foreach(var link in ((JArray) property.Value).OfType<JObject>())
                        {
                            yield return new JLink(link, property.Name);
                        }
                    }
                    else if(property.Value.Type == JTokenType.Object)
                    {
                        yield return new JLink((JObject) property.Value, property.Name);
                    }
                }
            }

            private IEnumerable<IResource> GetEmbedded()
            {
                var embedded = _inner.Value<JObject>("_embedded");

                foreach(var property in embedded?.Properties() ?? Enumerable.Empty<JProperty>())
                {
                    if(property.Value.Type == JTokenType.Array)
                    {
                        foreach(var resource in ((JArray) property.Value).OfType<JObject>())
                        {
                            yield return new JResource(resource, property.Name).WithBaseAddress(BaseAddress);
                        }
                    }
                    else if(property.Value.Type == JTokenType.Object)
                    {
                        yield return
                            new JResource((JObject) property.Value, property.Name).WithBaseAddress(BaseAddress);
                    }
                }
            }
        }

        private class JLink : ILink
        {
            private readonly JObject _inner;
            public string Rel { get; set; }

            public string Href
            {
                get => _inner.Value<string>("href");
                set => _inner["href"] = value;
            }

            public string Name
            {
                get => _inner.Value<string>("name");
                set => _inner["name"] = value;
            }

            public bool Templated
            {
                get => _inner.Value<bool>("templated");
                set => _inner["templated"] = value;
            }

            public string Title
            {
                get => _inner.Value<string>("title");
                set => _inner["title"] = value;
            }

            public JLink(JObject inner, string rel)
            {
                if(inner == null)
                    throw new ArgumentNullException(nameof(inner));
                _inner = inner;
                Rel = rel;
            }
        }
    }
}