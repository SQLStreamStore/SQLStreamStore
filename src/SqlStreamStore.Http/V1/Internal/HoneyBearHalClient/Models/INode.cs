namespace SqlStreamStore.V1.Internal.HoneyBearHalClient.Models
{
    using Newtonsoft.Json;

    internal interface INode
    {
        [JsonIgnore]
        string Rel { get; set; }

        [JsonProperty("href")]
        string Href { get; set; }

        [JsonProperty("name", NullValueHandling = NullValueHandling.Ignore)]
        string Name { get; set; }
    }
}