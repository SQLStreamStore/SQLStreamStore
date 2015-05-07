namespace Cedar.EventStore
{
    using System;

    public class StreamDeletedException : Exception
    {
        public StreamDeletedException(string storeId, Exception inner = null) 
            : base(string.Format("Event stream '{0}' is deleted.",storeId) ,inner)
        {}
    }
}