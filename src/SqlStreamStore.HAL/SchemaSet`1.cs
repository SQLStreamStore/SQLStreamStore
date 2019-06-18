namespace SqlStreamStore
{
    internal class SchemaSet<TResource> : SchemaSet where TResource : IResource
    {
        public SchemaSet() : base(typeof(TResource))
        { }
    }
}