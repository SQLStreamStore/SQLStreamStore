namespace SqlStreamStore.Subscriptions
{
    using System.Threading.Tasks;
    using SqlStreamStore;

    public delegate Task<IEventStoreNotifier> CreateEventStoreNotifier(IReadOnlyEventStore readOnlyEventStore);
}