namespace SqlStreamStore.Streams
{
    using System;

    public abstract class Pattern
    {
        private readonly string _value;

        public static Pattern StartsWith(string pattern) => new StartingWith(pattern);
        public static Pattern EndsWith(string pattern) => new EndingWith(pattern);
        public static Pattern Anything() => new Any();

        public static ArgumentException Unrecognized(string paramName)
            => new ArgumentException("Unrecognized pattern.", paramName);

        protected Pattern(string value)
        {
            _value = value;
        }

        public static explicit operator string(Pattern pattern) => pattern?._value;
        public override string ToString() => _value;

        public class StartingWith : Pattern
        {
            protected internal StartingWith(string value) : base(value)
            { }

            public static implicit operator string(StartingWith pattern) => pattern?._value;
        }

        public class EndingWith : Pattern
        {
            protected internal EndingWith(string value) : base(value)
            { }
            public static implicit operator string(EndingWith pattern) => pattern?._value;
        }

        public class Any : Pattern
        {
            protected internal Any() : base(null)
            { }
        }
    }
}