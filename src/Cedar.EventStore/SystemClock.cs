namespace Cedar.EventStore
{
    using System;

    public static class SystemClock
    {
        public static readonly GetUtcNow GetUtcNow = () => DateTimeOffset.UtcNow;
    }
}