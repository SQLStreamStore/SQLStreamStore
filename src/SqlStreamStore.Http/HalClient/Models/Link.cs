namespace SqlStreamStore.HalClient.Models
{
    internal sealed class Link : ILink
    {
        public string Rel { get; set; }
        public string Href { get; set; }
        public string Name { get; set; }
        public bool Templated { get; set; }

        public override string ToString() => $"Rel={Rel},Href={Href},Templated={Templated}";
    }
}