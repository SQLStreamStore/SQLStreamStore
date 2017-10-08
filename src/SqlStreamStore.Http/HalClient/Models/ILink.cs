namespace SqlStreamStore.HalClient.Models
{
    using Newtonsoft.Json;

    /// <summary>
    /// Represents a HAL link relation.
    /// </summary>
    internal interface ILink : INode
    {
        /// <summary>
        /// Specifies whether or not the link has templated parameters.
        /// </summary>
        [JsonProperty("templated", DefaultValueHandling = DefaultValueHandling.Ignore)]
        bool Templated { get; set; }
    }
}