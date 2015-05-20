namespace Cedar.EventStore
{
    using EnsureThat;

    public class Checkpoint
    {
        public static readonly Checkpoint Start = new StartCheckpoint();
        public static readonly Checkpoint End = new EndCheckpoint();
        private readonly string _value;

        public Checkpoint(string value)
        {
            Ensure.That(value).IsNotNullOrWhiteSpace();

            _value = value;
        }

        private Checkpoint()
        {}

        public string Value
        {
            get { return _value; }
        }

        public override string ToString()
        {
            return _value;
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