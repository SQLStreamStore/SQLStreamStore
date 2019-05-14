namespace SqlStreamStore.HAL.Index
{
    using System;
    using System.Linq;
    using System.Reflection;
    using Halcyon.HAL;
    using Microsoft.AspNetCore.Http;
    using Newtonsoft.Json;
    using Newtonsoft.Json.Linq;

    internal class IndexResource : IResource
    {
        public SchemaSet Schema { get; }

        private readonly JObject _data;

        public IndexResource(IStreamStore streamStore, Assembly serverAssembly)
        {
            var streamStoreType = streamStore.GetType();
            var streamStoreTypeName = streamStoreType.Name;
            var versions = JObject.FromObject(new
            {
                streamStore = GetVersion(streamStoreType)
            });
            versions[serverAssembly?.GetName().Name?.Split('.').LastOrDefault() ?? "Server"] =
                GetVersion(serverAssembly);

            _data = JObject.FromObject(new
            {
                provider = streamStoreTypeName.Substring(0, streamStoreTypeName.Length - "StreamStore".Length),
                versions
            });
        }

        private static string GetVersion(Type type) => GetVersion(type.Assembly);

        private static string GetVersion(Assembly assembly)
            => assembly
                   ?.GetCustomAttribute<AssemblyInformationalVersionAttribute>()
                   ?.InformationalVersion
               ?? assembly
                   ?.GetCustomAttribute<AssemblyVersionAttribute>()
                   ?.Version
               ?? "unknown";

        public Response Get() => new HalJsonResponse(new HALResponse(_data)
            .AddLinks(
                Links
                    .FromPath(PathString.Empty)
                    .Index().Self()
                    .Find()
                    .Browse()
                    .Add(Constants.Relations.Feed, LinkFormatter.AllStream())));

        public override string ToString() => _data.ToString(Formatting.None);
    }
}