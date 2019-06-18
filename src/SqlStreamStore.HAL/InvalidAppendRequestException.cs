namespace SqlStreamStore
{
    using System;

    internal class InvalidAppendRequestException : Exception
    {
        public InvalidAppendRequestException(string message)
            : base(message)
        { }

        public InvalidAppendRequestException(string message, Exception inner)
            : base(message, inner)
        { }
    }
}