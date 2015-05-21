namespace Cedar.EventStore.Exceptions
{
    using System;

    public class StreamDeletedException : Exception
    {
        public StreamDeletedException(string streamId, Exception inner = null) 
            : base(string.Format("Event stream '{0}' is deleted.", streamId) ,inner)
        {}
    }
}