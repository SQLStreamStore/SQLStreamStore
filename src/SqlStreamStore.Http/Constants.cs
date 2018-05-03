namespace SqlStreamStore
{
    internal static class Constants
    {
        public static class Direction
        {
            public const int Forwards = 1;
            public const int Backwards = -1;
        }

        public static class Headers
        {
            public const string HeadPosition = "SSS-HeadPosition";
            public const string ExpectedVersion = "SSS-ExpectedVersion";
        }
    }
}