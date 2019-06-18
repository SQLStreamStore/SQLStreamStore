namespace SqlStreamStore.V1
{
    using Newtonsoft.Json;
    using Newtonsoft.Json.Linq;
    using SqlStreamStore.V1.Streams;

    internal class HalStreamMetadataResult
    {
        public int MetadataStreamVersion { get; set; }
        public string StreamId { get; set; }
        public JObject MetadataJson { get; set; }
        public int? MaxCount { get; set; }
        public int? MaxAge { get; set; }

        public static implicit operator StreamMetadataResult(HalStreamMetadataResult result)
            => new StreamMetadataResult(
                result.StreamId,
                result.MetadataStreamVersion,
                result.MaxAge,
                result.MaxCount,
                result.MetadataJson?.ToString(Formatting.Indented));
    }
}