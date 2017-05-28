namespace SqlStreamStore.Subscriptions
{
    using System;
    using SqlStreamStore.Infrastructure;

    /// <summary>
    ///     Represents an notifier lets subsribers know that the 
    ///     stream store has new messages.
    /// </summary>
    public interface IStreamStoreNotifier : IObservable<Unit>, IDisposable
    {}
}