namespace Cedar.EventStore
{
    using System;

    public class StreamDeletedException : Exception
    {
        public StreamDeletedException(string storeId, string streamId, Exception inner = null) 
            : base(
                string.Format(
                    "Event stream '{0}' in '{1} is deleted.",
                    storeId,
                    streamId)
                ,inner)
        {}
    }
}