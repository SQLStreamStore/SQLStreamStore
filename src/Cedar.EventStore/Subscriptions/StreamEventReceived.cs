namespace Cedar.EventStore.Subscriptions
{
    using System.Threading.Tasks;
    using Cedar.EventStore.Streams;

    public delegate Task StreamEventReceived(StreamEvent streamEvent);
}