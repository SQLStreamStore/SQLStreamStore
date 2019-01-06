namespace SqlStreamStore
{
    using System;
    using SqlStreamStore.Infrastructure;

    public interface IStreamStoreFixture : IDisposable
    {
        IStreamStore Store { get; }

        GetUtcNow GetUtcNow { get; set; }
    }
}