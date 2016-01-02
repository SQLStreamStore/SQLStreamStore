namespace Cedar.EventStore
{
    public interface ICheckpoint
    {
        string Value { get; }
    }
}