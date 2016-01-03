namespace Cedar.EventStore.Streams
{
    using System.Threading.Tasks;

    public delegate Task EventReceived(StreamEvent streamEvent);
}