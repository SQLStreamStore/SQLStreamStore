namespace SqlStreamStore.HAL.Index
{
    using System;
    using System.Reflection;
    using Halcyon.HAL;
    using Microsoft.AspNetCore.Http;
    using Newtonsoft.Json;
    using Newtonsoft.Json.Linq;

    internal class IndexResource : IResource
    {
        public SchemaSet Schema { get; }

        private readonly JObject _data;

        public IndexResource(IStreamStore streamStore)
        {
            var streamStoreType = streamStore.GetType();
            var streamStoreTypeName = streamStoreType.Name;
            _data = JObject.FromObject(new
            {
                provider = streamStoreTypeName.Substring(0, streamStoreTypeName.Length - "StreamStore".Length),
                versions = new
                {
                    streamStore = GetVersion(streamStoreType),
                    server = GetVersion(typeof(IndexResource))
                }
            });
        }

        private static string GetVersion(Type type)
            => type.Assembly
                   .GetCustomAttribute<AssemblyInformationalVersionAttribute>()
                   ?.InformationalVersion
               ?? type.Assembly
                   .GetCustomAttribute<AssemblyVersionAttribute>()
                   ?.Version
               ?? "unknown";

        public Response Get() => new HalJsonResponse(new HALResponse(_data)
            .AddLinks(
                Links
                    .FromPath(PathString.Empty)
                    .Index().Self()
                    .Find()
                    .Browse()
                    .Add(Constants.Relations.Feed, Constants.Streams.All)));

        public override string ToString() => _data.ToString(Formatting.None);
    }
}