namespace SqlStreamStore.Streams
{
    using System;

    public abstract class Pattern
    {
        public static Pattern StartsWith(string pattern) => new StartingWith(pattern);
        public static Pattern EndsWith(string pattern) => new EndingWith(pattern);
        public static Pattern Anything() => new Any();

        public static ArgumentException Unrecognized(string paramName)
            => new ArgumentException("Unrecognized pattern.", paramName);

        public string Value { get; }

        protected Pattern(string value)
        {
            Value = value;
        }

        public class StartingWith : Pattern
        {
            protected internal StartingWith(string value) : base(value)
            { }
        }

        public class EndingWith : Pattern
        {
            protected internal EndingWith(string value) : base(value)
            { }
        }

        public class Any : Pattern
        {
            protected internal Any() : base(null)
            { }
        }
    }
}