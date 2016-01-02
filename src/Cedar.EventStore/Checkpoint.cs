namespace Cedar.EventStore
{
    using System;
    using EnsureThat;

    public interface ICheckpoint : IComparable<ICheckpoint>
    {
        string Value { get; }
    }

    public class Checkpoint
    {
        public static readonly Checkpoint Start = new StartCheckpoint();
        public static readonly Checkpoint End = new EndCheckpoint();
        public readonly string Value;

        public Checkpoint(string value)
        {
            Ensure.That(value).IsNotNullOrWhiteSpace();

            Value = value;
        }

        private Checkpoint()
        {}

        public override string ToString()
        {
            return Value;
        }

        public class StartCheckpoint : Checkpoint
        {
            public override string ToString()
            {
                return "<Start>";
            }
        }

        public class EndCheckpoint : Checkpoint
        {
            public override string ToString()
            {
                return "<End>";
            }
        }

        public static implicit operator Checkpoint(string value)
        {
            return new Checkpoint(value);
        }
    }
}