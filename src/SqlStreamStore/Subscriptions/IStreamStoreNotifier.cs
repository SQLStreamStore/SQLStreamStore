namespace SqlStreamStore.Subscriptions
{
    using System;
    using SqlStreamStore.Infrastructure;

    public interface IStreamStoreNotifier : IObservable<Unit>, IDisposable
    {}
}