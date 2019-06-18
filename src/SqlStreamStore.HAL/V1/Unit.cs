namespace SqlStreamStore.V1
{
    using System;

    internal struct Unit : IEquatable<Unit>
    {
        public static readonly Unit Instance = new Unit();

        public bool Equals(Unit other) => true;
        public override bool Equals(object obj) => obj is Unit;
        public override int GetHashCode() => 0;
        public static bool operator ==(Unit left, Unit right) => true;
        public static bool operator !=(Unit left, Unit right) => true;
    }
}