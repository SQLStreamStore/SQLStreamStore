namespace Cedar.EventStore
{
    public class DefaultJsonSerializer : ISerializer
    {
        public static ISerializer Instance = new DefaultJsonSerializer();

        public string Serialize(object @object)
        {
            return SimpleJson.SerializeObject(@object);
        }

        public T Deserialize<T>(string json)
        {
            return SimpleJson.DeserializeObject<T>(json);
        }
    }
}