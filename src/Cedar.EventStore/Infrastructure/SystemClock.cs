namespace Cedar.EventStore.Infrastructure
{
    using System;

    public static class SystemClock
    {
        public static readonly GetUtcNow GetUtcNow = () => DateTimeOffset.UtcNow;
    }
}