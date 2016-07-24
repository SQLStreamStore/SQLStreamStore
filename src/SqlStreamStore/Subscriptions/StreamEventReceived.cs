namespace StreamStore.Subscriptions
{
    using System.Threading.Tasks;
    using StreamStore.Streams;

    public delegate Task StreamEventReceived(StreamEvent streamEvent);
}