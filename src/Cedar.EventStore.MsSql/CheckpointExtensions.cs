namespace Cedar.EventStore
{
    internal static class CheckpointExtensions
    {
        internal static long GetOrdinal(this Checkpoint checkpoint)
        {
            if(ReferenceEquals(checkpoint, Checkpoint.Start))
            {
                return -1;
            }
            return ReferenceEquals(checkpoint, Checkpoint.End) ? long.MaxValue : long.Parse(checkpoint.Value);
        }
    }
}