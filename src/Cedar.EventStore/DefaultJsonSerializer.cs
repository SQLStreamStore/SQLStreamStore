namespace Cedar.EventStore
{
    public class DefaultJsonSerializer : IJsonSerializer
    {
        public static IJsonSerializer Instance = new DefaultJsonSerializer();

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