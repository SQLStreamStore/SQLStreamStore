namespace SqlStreamStore.Infrastructure
{
    using System;

    public static class SystemClock
    {
        public static readonly GetUtcNow GetUtcNow = () => DateTime.UtcNow;
    }
}