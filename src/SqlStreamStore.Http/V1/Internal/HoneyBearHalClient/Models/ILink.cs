namespace SqlStreamStore.V1.Internal.HoneyBearHalClient.Models
{
    using Newtonsoft.Json;

    internal interface ILink : INode
    {
        [JsonProperty("templated", DefaultValueHandling = DefaultValueHandling.Ignore)]
        bool Templated { get; set; }

        [JsonProperty("title")]
        string Title { get; set; }
    }
}