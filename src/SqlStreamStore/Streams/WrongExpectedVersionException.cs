namespace SqlStreamStore.Streams
{
    using System;

    /// <summary>
    ///     Represents an exception that is thrown when a version supplied
    ///     as part of an append does not match the stream version
    ///     (part of concurrency control).
    /// </summary>
    public class WrongExpectedVersionException : Exception
    {
        /// <summary>
        ///     Initializes a new instance of <see cref="WrongExpectedVersionException"/>.
        /// </summary>
        /// <param name="message"></param>
        /// <param name="inner"></param>
        public WrongExpectedVersionException(string message, Exception inner = null)
            : base(message, inner)
        {}
    }
}