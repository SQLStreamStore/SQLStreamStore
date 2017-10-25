namespace SqlStreamStore
{
    using System;
    using SqlStreamStore.Streams;

    internal struct MySqlOrdinal : IEquatable<MySqlOrdinal>
    {
        private readonly long _position;
        private readonly long _ordinal;
        
        public static MySqlOrdinal CreateFromMySqlOrdinal(long ordinal) => new MySqlOrdinal(ordinal - 1, ordinal);

        public static MySqlOrdinal CreateFromStreamStorePosition(long position) 
            => position == Position.End 
                ? new MySqlOrdinal(Position.End, long.MaxValue) 
                : new MySqlOrdinal(position, position + 1);

        private MySqlOrdinal(long position, long ordinal)
        {
            _position = position;
            _ordinal = ordinal;
        }

        public override int GetHashCode() => _position.GetHashCode();
        public override bool Equals(object obj) => (obj is MySqlOrdinal) && Equals((MySqlOrdinal) obj);
        public bool Equals(MySqlOrdinal other) => other._position == _position && other._ordinal == _ordinal;
        public static bool operator ==(MySqlOrdinal left, MySqlOrdinal right) => left.Equals(right);
        public static bool operator !=(MySqlOrdinal left, MySqlOrdinal right) => !left.Equals(right);
        public override string ToString() => $"SqlStreamStore Position: {_position}; MySqlOrdinal: {_ordinal}";

        public long ToStreamStorePosition() => _position;

        public long ToMySqlOrdinal() => _ordinal;
    }
}