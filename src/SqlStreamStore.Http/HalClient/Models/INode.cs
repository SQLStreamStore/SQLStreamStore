namespace SqlStreamStore.HalClient.Models
{
    using Newtonsoft.Json;

    /// <summary>
    /// Represents a generic HAL element.
    /// </summary>
    internal interface INode
    {
        /// <summary>
        /// The relationship name.
        /// </summary>
        [JsonIgnore]
        string Rel { get; set; }

        /// <summary>
        /// The URI of the element.
        /// </summary>
        [JsonProperty("href")]
        string Href { get; set; }

        /// <summary>
        /// The name of the element.
        /// </summary>
        [JsonProperty("name", NullValueHandling = NullValueHandling.Ignore)]
        string Name { get; set; }
    }
}