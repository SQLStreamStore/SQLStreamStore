namespace Cedar.EventStore.Subscriptions
{
    using System;
    using Cedar.EventStore.Infrastructure;

    public abstract class SubscriptionBase
    {
        private int _pageSize;

        protected SubscriptionBase(
            IReadOnlyEventStore readOnlyEventStore,
            IObservable<Unit> eventStoreAppendedNotification,
            StreamEventReceived streamEventReceived,
            SubscriptionDropped subscriptionDropped = null,
            string name = null)
        {
            ReadOnlyEventStore = readOnlyEventStore;
            Name = string.IsNullOrWhiteSpace(name) ? Guid.NewGuid().ToString() : name;
            SubscriptionDropped = subscriptionDropped ?? ((_, __) => { });
        }

        public string Name { get; }

        public int PageSize
        {
            get { return _pageSize; }
            set { _pageSize = (value <= 0) ? 1 : value; }
        }

        protected IReadOnlyEventStore ReadOnlyEventStore { get; }

        protected SubscriptionDropped SubscriptionDropped { get; }
    }
}
