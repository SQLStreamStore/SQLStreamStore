namespace SqlStreamStore.V1.Internal.HoneyBearHalClient.Models
{
    internal interface IResource<out T> : IResource
        where T : class, new()
    {
        T Data { get; }
    }
}