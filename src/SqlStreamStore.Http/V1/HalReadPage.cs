namespace SqlStreamStore.V1
{
    internal class HalReadPage
    {
        public int FromStreamVersion { get; set; }
        public int LastStreamVersion { get; set; }
        public int NextStreamVersion { get; set; }
        public long LastStreamPosition { get; set; }
        public bool IsEnd { get; set; }
    }
}