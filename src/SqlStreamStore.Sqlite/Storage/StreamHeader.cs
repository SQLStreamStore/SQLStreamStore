namespace SqlStreamStore
{
    public class StreamHeader
    {
        public string Id { get; set; }
        public int Key { get; set; }
        public int Version { get; set; }
        public long Position { get; set; }
        public int? MaxAge { get; set; }
        public int? MaxCount { get; set; }
    }
}