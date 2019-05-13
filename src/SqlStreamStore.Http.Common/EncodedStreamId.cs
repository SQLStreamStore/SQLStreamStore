namespace SqlStreamStore
{
    using System;

    internal struct EncodedStreamId : IEquatable<EncodedStreamId>
    {
        private const string EncodedForwardSlash = "%2f";

        private readonly string _value;

        public EncodedStreamId(string streamId)
        {
            if(streamId == null)
            {
                throw new ArgumentNullException(nameof(streamId));
            }

            _value = Uri.EscapeDataString(streamId.Replace("/", EncodedForwardSlash));
        }

        public bool Equals(EncodedStreamId other) => string.Equals(_value, other._value);
        public override bool Equals(object obj) => obj is EncodedStreamId other && Equals(other);
        public override int GetHashCode() => _value.GetHashCode();
        public static bool operator ==(EncodedStreamId left, EncodedStreamId right) => left.Equals(right);
        public static bool operator !=(EncodedStreamId left, EncodedStreamId right) => !left.Equals(right);
        public static implicit operator string(EncodedStreamId streamId) => streamId._value;

        public override string ToString() => _value;
    }
}