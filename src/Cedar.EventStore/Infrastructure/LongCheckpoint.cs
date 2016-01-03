namespace Cedar.EventStore.Infrastructure
{
    using System.Globalization;

    public sealed class LongCheckpoint : ICheckpoint
    {
        public static readonly ICheckpoint End = new LongCheckpoint(long.MaxValue);
        public static readonly ICheckpoint Start = new LongCheckpoint(0);

        public LongCheckpoint(long value)
        {
            LongValue = value;
        }

        public long LongValue { get; }

        public string Value => LongValue.ToString(CultureInfo.InvariantCulture);

        public static LongCheckpoint Parse(string checkpointValue)
        {
            return string.IsNullOrWhiteSpace(checkpointValue)
                ? new LongCheckpoint(-1)
                : new LongCheckpoint(long.Parse(checkpointValue));
        }

        public override string ToString()
        {
            return Value;
        }
    }
}