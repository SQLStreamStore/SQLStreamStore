namespace SqlStreamStore.Subscriptions
{
    using SqlStreamStore;

    public delegate IStreamStoreNotifier CreateStreamStoreNotifier(IReadonlyStreamStore readonlyStreamStore);
}