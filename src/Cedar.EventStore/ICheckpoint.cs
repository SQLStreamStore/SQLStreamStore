namespace Cedar.EventStore
{
    using System;

    public interface ICheckpoint : IComparable<ICheckpoint>
    {
        string Value { get; }
    }
}