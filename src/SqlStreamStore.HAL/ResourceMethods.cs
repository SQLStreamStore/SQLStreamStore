namespace SqlStreamStore.HAL
{
    using System.Linq;
    using System.Net.Http;
    using System.Reflection;
    using System.Threading.Tasks;

    internal static class ResourceMethods
    {
        public static HttpMethod[] Discover(IResource resource)
        {
            var httpMethods = resource.GetType()
                .GetMethods(
                    BindingFlags.Public | BindingFlags.Instance | BindingFlags.DeclaredOnly)
                .Where(method => method.ReturnType == typeof(Response) || method.ReturnType == typeof(Task<Response>))
                .Select(method => new HttpMethod(method.Name.ToUpperInvariant()))
                .Concat(new[] { HttpMethod.Options })
                .ToList();

            if(httpMethods.Contains(HttpMethod.Get))
            {
                httpMethods.Add(HttpMethod.Head);
            }

            return httpMethods.ToArray();
        }
    }
}