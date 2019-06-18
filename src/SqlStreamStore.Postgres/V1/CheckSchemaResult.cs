namespace SqlStreamStore.V1
{
    using System;

    public struct CheckSchemaResult : IEquatable<CheckSchemaResult>
    {
        public int CurrentVersion { get; }
        public int ExpectedVersion { get; }
        public bool IsMatch => CurrentVersion == ExpectedVersion;

        public CheckSchemaResult(int currentVersion, int expectedVersion)
        {
            CurrentVersion = currentVersion;
            ExpectedVersion = expectedVersion;
        }

        public bool Equals(CheckSchemaResult other) 
            => CurrentVersion == other.CurrentVersion && ExpectedVersion == other.ExpectedVersion;
        public override bool Equals(object obj) 
            => obj is CheckSchemaResult other && Equals(other);
        public static bool operator ==(CheckSchemaResult left, CheckSchemaResult right) 
            => left.Equals(right);
        public static bool operator !=(CheckSchemaResult left, CheckSchemaResult right) 
            => !left.Equals(right);

        public override int GetHashCode()
        {
            unchecked
            {
                return (CurrentVersion * 397) ^ ExpectedVersion;
            }
        }
    }
}