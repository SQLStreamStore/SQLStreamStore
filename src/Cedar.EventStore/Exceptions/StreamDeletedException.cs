namespace Cedar.EventStore.Exceptions
{
    using System;

    public class StreamDeletedException : Exception
    {
        public StreamDeletedException(string message, Exception inner = null) 
            : base(message,inner)
        {}
    }
}