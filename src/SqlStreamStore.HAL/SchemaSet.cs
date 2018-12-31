namespace SqlStreamStore.HAL
{
    using System;
    using System.Collections.Concurrent;
    using System.IO;
    using System.Reflection;
    using Halcyon.HAL;
    using Newtonsoft.Json;
    using Newtonsoft.Json.Linq;

    internal abstract class SchemaSet
    {
        private static readonly ConcurrentDictionary<string, HALResponse> s_schemas
            = new ConcurrentDictionary<string, HALResponse>();

        private readonly Type _resourceType;

        protected SchemaSet(Type resourceType)
        {
            if(resourceType == null)
                throw new ArgumentNullException(nameof(resourceType));
            _resourceType = resourceType;
        }

        public HALResponse GetSchema(string name) => s_schemas.GetOrAdd(name, ReadSchema);

        public Stream GetDocumentation(string name) => GetStream(name, "md");

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