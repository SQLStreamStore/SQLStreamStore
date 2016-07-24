namespace SqlStreamStore.Subscriptions
{
    using System.Threading.Tasks;
    using SqlStreamStore;

    public delegate Task<IStreamStoreNotifier> CreateStreamStoreNotifier(IReadonlyStreamStore readonlyStreamStore);
}