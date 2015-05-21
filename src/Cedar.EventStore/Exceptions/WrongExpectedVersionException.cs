namespace Cedar.EventStore.Exceptions
{
    using System;

    public class WrongExpectedVersionException : Exception
    {
        public WrongExpectedVersionException(string streamId, int expectedVersion, Exception inner = null)
            : base(string.Format(
                "Delete stream failed due to WrongExpectedVersion. Stream: {0}, Expected version: {1}", streamId, expectedVersion),
                inner)
        {}
    }
}