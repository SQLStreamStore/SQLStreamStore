namespace SqlStreamStore.Internal.HoneyBearHalClient.Models
{
    using Newtonsoft.Json;

    internal interface ILink : INode
    {
        [JsonProperty("templated", DefaultValueHandling = DefaultValueHandling.Ignore)]
        bool Templated { get; set; }
    }
}