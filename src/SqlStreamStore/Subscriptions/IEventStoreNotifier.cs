namespace StreamStore.Subscriptions
{
    using System;
    using StreamStore.Infrastructure;

    public interface IEventStoreNotifier : IObservable<Unit>, IDisposable
    {}
}