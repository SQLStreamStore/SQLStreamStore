namespace SqlStreamStore.Internal.HoneyBearHalClient
{
    using System;
    using System.IO;
    using System.Linq;
    using System.Net.Http;
    using System.Threading;
    using System.Threading.Tasks;
    using Newtonsoft.Json;
    using SqlStreamStore.Internal.HoneyBearHalClient.Models;
    using SqlStreamStore.Internal.HoneyBearHalClient.Serialization;
    using Tavis.UriTemplates;

    internal static class HalClientExtensions
    {
        public static Task<IHalClient> BuildAndExecuteAsync(
            this IHalClient client,
            string relationship,
            object parameters,
            Func<string, Task<HttpResponseMessage>> command)
        {
            var resource = client.Current.FirstOrDefault(r => r.Links.Any(l => l.Rel == relationship));
            if(resource == null)
                throw new FailedToResolveRelationship(relationship);

            var link = resource.Links.FirstOrDefault(l => l.Rel == relationship);
            return ExecuteAsync(client, Construct(resource.BaseAddress, link, parameters), command);
        }

        public static async Task<IHalClient> ExecuteAsync(
            this IHalClient client,
            string uri,
            Func<string, Task<HttpResponseMessage>> command)
        {
            var result = await command(uri);

            var current =
                new[]
                {
                    (result.Content.Headers.ContentLength == 0
                        ? new Resource()
                        : await result.Content.ReadResource())
                    .WithBaseAddress(new Uri(client.Client.BaseAddress, uri))
                };

            return new HalClient(client, current, result.StatusCode);
        }

        private static async Task<IResource> ReadResource(this HttpContent content)
        {
            var stream = await content.ReadAsStreamAsync();

            using(var reader = new JsonTextReader(new StreamReader(stream))
            {
                CloseInput = false
            })
            {
                return await HalResourceJsonReader.ReadResource(reader, CancellationToken.None);
            }
        }

        public static string Construct(Uri baseAddress, ILink link, object parameters)
        {
            if(!link.Templated)
                return new Uri(baseAddress, link.Href).ToString();

            if(parameters == null)
                throw new TemplateParametersAreRequired(link);

            var template = new UriTemplate(
                new Uri(baseAddress, link.Href).ToString(),
                caseInsensitiveParameterNames: true);
            template.AddParameters(parameters);
            return template.Resolve();
        }

        public static string Relationship(string rel, string curie) => curie == null ? rel : $"{curie}:{rel}";
    }
}