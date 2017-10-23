namespace SqlStreamStore
{
    using System;
    using SqlStreamStore.Streams;

    internal struct MySqlOrdinal : IEquatable<MySqlOrdinal>
    {
        private readonly long _value;
        
        public static MySqlOrdinal CreateFromMySqlOrdinal(long position) => new MySqlOrdinal(position - 1);

        public static MySqlOrdinal CreateFromStreamStorePosition(long position) => new MySqlOrdinal(position);

        private MySqlOrdinal(long value)
        {
            _value = value;
        }

        public override int GetHashCode() => _value.GetHashCode();
        public override bool Equals(object obj) => (obj is MySqlOrdinal) && Equals((MySqlOrdinal) obj);
        public bool Equals(MySqlOrdinal mySqlOrdinal) => mySqlOrdinal._value == _value;
        public static bool operator ==(MySqlOrdinal left, MySqlOrdinal right) => left.Equals(right);
        public static bool operator !=(MySqlOrdinal left, MySqlOrdinal right) => !left.Equals(right);

        public long ToStreamStorePosition() => _value;

        public long ToMySqlOrdinal() => _value == Position.End || _value == long.MaxValue
            ? long.MaxValue
            : _value + 1;
    }
}