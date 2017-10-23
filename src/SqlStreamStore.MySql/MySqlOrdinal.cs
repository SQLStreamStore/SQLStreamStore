namespace SqlStreamStore
{
    using System;

    internal struct MySqlOrdinal : IEquatable<MySqlOrdinal>
    {
        private readonly long _position;
        private readonly long _ordinal;
        
        public static MySqlOrdinal CreateFromMySqlOrdinal(long ordinal) => new MySqlOrdinal(ordinal - 1, ordinal);

        public static MySqlOrdinal CreateFromStreamStorePosition(long position) => new MySqlOrdinal(position, position + 1);

        private MySqlOrdinal(long position, long ordinal)
        {
            if(position + 1 != ordinal)
            {
                throw new ArgumentOutOfRangeException(nameof(ordinal));
            }
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