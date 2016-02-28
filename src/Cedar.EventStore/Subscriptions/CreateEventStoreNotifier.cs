namespace Cedar.EventStore.Subscriptions
{
    using System.Threading.Tasks;

    public delegate Task<IEventStoreNotifier> CreateEventStoreNotifier(IReadOnlyEventStore readOnlyEventStore);
}