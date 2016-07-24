namespace SqlStreamStore.Subscriptions
{
    using System;
    using SqlStreamStore.Infrastructure;

    public interface IEventStoreNotifier : IObservable<Unit>, IDisposable
    {}
}