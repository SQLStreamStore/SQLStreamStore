namespace Cedar.EventStore.Subscriptions
{
    using System;
    using Cedar.EventStore.Infrastructure;

    public interface IEventStoreNotifier : IObservable<Unit>, IDisposable
    {}
}