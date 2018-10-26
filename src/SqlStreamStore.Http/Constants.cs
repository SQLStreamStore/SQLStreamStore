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

        public static class Relations
        {
            public const string Self = "self";
            public const string First = "first";
            public const string Previous = "previous";
            public const string Next = "next";
            public const string Last = "last";
            public const string Index = "streamStore:index";
            public const string Feed = "streamStore:feed";
            public const string Message = "streamStore:message";
            public const string Metadata = "streamStore:metadata";
            public const string AppendToStream = "streamStore:append";
            public const string DeleteStreamMessage = "streamStore:delete-message";
            public const string DeleteStream = "streamStore:delete-stream";
        }
    }
}