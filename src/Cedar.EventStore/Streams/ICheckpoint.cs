namespace Cedar.EventStore.Streams
{
    public interface ICheckpoint
    {
        string Value { get; }
    }
}