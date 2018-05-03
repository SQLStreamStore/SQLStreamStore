namespace SqlStreamStore
{
    internal class HalReadAllPage
    {
        public long FromPosition { get; set; }
        public long NextPosition { get; set; }
        public bool IsEnd { get; set; }
    }
}