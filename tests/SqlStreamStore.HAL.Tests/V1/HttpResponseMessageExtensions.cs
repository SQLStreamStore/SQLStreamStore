namespace SqlStreamStore.V1
{
    using System;
    using System.Collections.Generic;
    using System.Net.Http;
    using System.Threading.Tasks;
    using Newtonsoft.Json.Linq;

    internal static class HttpResponseMessageExtensions
    {
        public static async Task<Resource> AsHal(this HttpResponseMessage response)
            => ParseResource(JObject.Parse(await response.Content.ReadAsStringAsync()));

        /// <summary>
        /// https://github.com/wis3guy/HalClient.Net/blob/master/HalClient.Net/Parser/HalJsonParser.cs
        /// </summary>
        /// <param name="outer"></param>
        /// <returns></returns>
        private static Resource ParseResource(JObject outer)
        {
            var links = new List<Link>();
            var embedded = new List<(string, Resource)>();
            var state = new JObject();

            foreach(var inner in outer.Properties())
            {
                var type = inner.Value.Type.ToString();

                if(inner.Value.Type == JTokenType.Object)
                {
                    var value = (JObject) inner.Value;

                    switch(inner.Name)
                    {
                        case "_links":
                            links.AddRange(ParseObjectOrArrayOfObjects(value, ParseLink));
                            break;
                        case "_embedded":
                            embedded.AddRange(ParseObjectOrArrayOfObjects(value, ParseEmbeddedResource));
                            break;
                        default:
                            state.Add(inner.Name, inner.Value);
                            break;
                    }
                }
                else
                {
                    var value = inner.Value.ToString();

                    switch(inner.Name)
                    {
                        case "_links":
                        case "_embedded":
                            if(inner.Value.Type != JTokenType.Null)
                            {
                                throw new FormatException(
                                    $"Invalid value for {inner.Name}: {value}");
                            }
                            break;
                        default:
                            state.Add(inner.Name, inner.Value);
                            break;
                    }
                }
            }

            return new Resource(state, links.ToArray(), embedded.ToArray());
        }

        private static (string, Resource) ParseEmbeddedResource(JObject outer, string rel)
            => (rel, ParseResource(outer));

        private static Link ParseLink(JObject outer, string rel)
        {
            var link = new Link { Rel = rel };

            string href = null;

            foreach(var inner in outer.Properties())
            {
                var value = inner.Value.ToString();

                if(string.IsNullOrEmpty(value))
                    continue; // nothing to assign, just leave the default value ...

                var attribute = inner.Name.ToLowerInvariant();

                switch(attribute)
                {
                    case "href":
                        href = value;
                        break;
                    case "templated":
                        //link.Templated = value.Equals("true", StringComparison.OrdinalIgnoreCase);
                        break;
                    case "type":
                        //link.Type = value;
                        break;
                    case "deprication":
                        //link.SetDeprecation(value);
                        break;
                    case "name":
                        //link.Name = value;
                        break;
                    case "profile":
                        //link.SetProfile(value);
                        break;
                    case "title":
                        link.Title = value;
                        break;
                    case "hreflang":
                        //link.HrefLang = value;
                        break;
                    default:
                        throw new NotSupportedException("Unsupported link attribute encountered: " + attribute);
                }
            }

            link.Href = href;

            return link;
        }

        private static IEnumerable<T> ParseObjectOrArrayOfObjects<T>(JObject outer, Func<JObject, string, T> factory)
        {
            foreach(var inner in outer.Properties())
            {
                var rel = inner.Name;

                if(inner.Value.Type == JTokenType.Array)
                    foreach(var child in inner.Value.Children<JObject>())
                        yield return factory(child, rel);
                else
                    yield return factory((JObject) inner.Value, rel);
            }
        }
    }
}