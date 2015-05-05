namespace Cedar.EventStore
{
    public interface ISerializer
    {
        string Serialize(object @object);

        T Deserialize<T>(string json);
    }
}