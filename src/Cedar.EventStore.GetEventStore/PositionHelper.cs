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

        internal static Position? ParsePosition(this string checkpointToken)
        {
            if (string.IsNullOrWhiteSpace(checkpointToken))
            {
                return default(Position?);
            }

            var positions = checkpointToken.Split(new[] { '/' }, 2).Select(s => s.Trim()).ToArray();
            if (positions.Length != 2)
            {
                throw new ArgumentException();
            }

            return new Position(long.Parse(positions[0]), long.Parse(positions[1])); ;
        }
    }
}