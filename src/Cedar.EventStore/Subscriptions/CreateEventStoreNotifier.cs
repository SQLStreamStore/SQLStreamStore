namespace StreamStore.Subscriptions
{
    using System.Threading.Tasks;
    using StreamStore;

    public delegate Task<IEventStoreNotifier> CreateEventStoreNotifier(IReadOnlyEventStore readOnlyEventStore);
}