namespace SqlStreamStore.V1
{
    using System.Reflection;

    public class SqlStreamStoreMiddlewareOptions
    {
        public bool UseCanonicalUrls { get; set; } = true;
        public Assembly ServerAssembly { get; set; }
    }
}