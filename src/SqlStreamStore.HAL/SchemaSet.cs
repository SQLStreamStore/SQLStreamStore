namespace SqlStreamStore.HAL
{
    using System;
    using System.Collections.Concurrent;
    using System.Collections.Generic;
    using System.IO;
    using System.Reflection;
    using Halcyon.HAL;
    using Newtonsoft.Json;
    using Newtonsoft.Json.Linq;

    internal abstract class SchemaSet
    {
        private static readonly ConcurrentDictionary<string, HALResponse> s_schemas
            = new ConcurrentDictionary<string, HALResponse>();

        private static readonly IDictionary<string, string> s_mediaTypeToExtension = new Dictionary<string, string>
        {
            [Constants.MediaTypes.JsonHyperSchema] = "json",
            [Constants.MediaTypes.TextMarkdown] = "md",
            [Constants.MediaTypes.Any] = "md",
            ["*"] = "md"
        };

        private readonly Type _resourceType;

        protected SchemaSet(Type resourceType)
        {
            if(resourceType == null)
                throw new ArgumentNullException(nameof(resourceType));
            _resourceType = resourceType;
        }

        public HALResponse GetSchema(string name) => s_schemas.GetOrAdd(name, ReadSchema);

        public Stream GetDocumentation(string name, string mediaType)
            => s_mediaTypeToExtension.TryGetValue(mediaType, out var extension)
                ? GetStream(name, extension)
                : null;

        private HALResponse ReadSchema(string name)
        {
            using(var stream = GetStream(name, "json")
                               ?? throw new Exception($"Embedded schema resource, {name}, not found. BUG!"))
            using(var reader = new JsonTextReader(new StreamReader(stream)))
            {
                return new HALResponse(JObject.Load(reader));
            }
        }

        private Stream GetStream(string name, string extension)
            => _resourceType
                .GetTypeInfo().Assembly
                .GetManifestResourceStream(_resourceType, $"Schema.{name}.schema.{extension}");
    }
}