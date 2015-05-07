namespace Cedar.EventStore
{
    using global::EventStore.ClientAPI;

    public delegate IEventStoreConnection CreateEventStoreConnection();
}