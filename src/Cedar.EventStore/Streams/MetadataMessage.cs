namespace StreamStore.Streams
{
    public class MetadataMessage
    {
        public string StreamId;
        public int? MaxAge;
        public int? MaxCount;
        public string MetaJson;

        public static string MetadataEventType = "$stream-metadata";
    }
}