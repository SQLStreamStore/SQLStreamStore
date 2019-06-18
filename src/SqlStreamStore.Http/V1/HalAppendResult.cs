namespace SqlStreamStore.V1
{
    using SqlStreamStore.V1.Streams;

    internal class HalAppendResult
    {
        public int CurrentVersion { get; set; }
        public long CurrentPosition { get; set; }

        public static implicit operator AppendResult(HalAppendResult result)
            => new AppendResult(result.CurrentVersion, result.CurrentPosition);
    }
}