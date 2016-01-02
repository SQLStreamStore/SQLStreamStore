namespace Cedar.EventStore
{
    using System.Linq;
    using global::EventStore.ClientAPI;

    internal sealed class PositionCheckpoint : ICheckpoint
    {
        internal static readonly ICheckpoint Start = new PositionCheckpoint(Position.Start);
        internal static readonly ICheckpoint End = new PositionCheckpoint(Position.End);

        private readonly Position _position;

        private PositionCheckpoint(Position position)
        {
            _position = position;
        }

        public string Value => _position.ToString();

        internal Position Position => _position;

        public static PositionCheckpoint Parse(string checkpointValue)
        {
            var positions = checkpointValue.Split(new[] { '/' }, 2).Select(s => s.Trim()).ToArray();
            var position = new Position(long.Parse(positions[0]), long.Parse(positions[1]));
            return new PositionCheckpoint(position);
        }
    }
}