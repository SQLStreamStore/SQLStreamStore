namespace Cedar.EventStore
{
    public static class DefaultJsonSerializer
    {
        public static string Serialize(object @object)
        {
            return SimpleJson.SerializeObject(@object);
        }

        public static T Deserialize<T>(string json)
        {
            return SimpleJson.DeserializeObject<T>(json);
        }
    }
}