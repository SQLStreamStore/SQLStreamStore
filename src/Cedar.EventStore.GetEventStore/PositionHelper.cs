namespace Cedar.EventStore
{
    using System;
    using System.Linq;
    using global::EventStore.ClientAPI;

    internal static class PositionHelper
    {
        internal static string ToCheckpoint(this Position? position)
        {
            return position.HasValue
                ? position.Value.CommitPosition + "/" +
                  position.Value.PreparePosition
                : null;
        }

        internal static Position ParsePosition(this Checkpoint checkpoint)
        {
            if(ReferenceEquals(checkpoint, Checkpoint.Start))
            {
                return Position.Start;
            }
            if(ReferenceEquals(checkpoint, Checkpoint.End))
            {
                return Position.End;
            }
            var positions = checkpoint.Value.Split(new[] { '/' }, 2).Select(s => s.Trim()).ToArray();
            if (positions.Length != 2)
            {
                throw new ArgumentException("Unable to parse checkpoint");
            }

            return new Position(long.Parse(positions[0]), long.Parse(positions[1])); ;
        }
    }
}