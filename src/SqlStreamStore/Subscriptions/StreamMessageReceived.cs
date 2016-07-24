namespace SqlStreamStore.Subscriptions
{
    using System.Threading.Tasks;
    using SqlStreamStore.Streams;

    public delegate Task StreamMessageReceived(StreamMessage streamMessage);
}