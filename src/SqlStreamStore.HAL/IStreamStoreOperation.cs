namespace SqlStreamStore.HAL
{
    using System.Threading;
    using System.Threading.Tasks;
    using Microsoft.AspNetCore.Http;
    using SqlStreamStore.Streams;

    internal interface IStreamStoreOperation<T>
    {
        PathString Path { get; }
        Task<T> Invoke(IStreamStore<ReadAllPage> streamStore, CancellationToken cancellationToken);
    }
}