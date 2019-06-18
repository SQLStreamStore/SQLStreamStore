namespace SqlStreamStore.V1
{
    internal class HalReadAllPage
    {
        public long FromPosition { get; set; }
        public long NextPosition { get; set; }
        public bool IsEnd { get; set; }
    }
}