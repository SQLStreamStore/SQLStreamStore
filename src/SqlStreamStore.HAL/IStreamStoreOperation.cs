namespace SqlStreamStore
{
    using System.Threading;
    using System.Threading.Tasks;
    using Microsoft.AspNetCore.Http;

    internal interface IStreamStoreOperation<T>
    {
        PathString Path { get; }
        Task<T> Invoke(IStreamStore streamStore, CancellationToken cancellationToken);
    }
}