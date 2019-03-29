namespace SqlStreamStore.HAL
{
    using System.Linq;
    using System.Reflection;
    using Microsoft.AspNetCore.Http;
    using SqlStreamStore.Streams;

    internal static class Constants
    {
        public static class MediaTypes
        {
            public const string TextMarkdown = "text/markdown";
            public const string HalJson = "application/hal+json";
            public const string JsonHyperSchema = "application/schema+json";
            public const string Any = "*/*";
        }

        public static class Headers
        {
            public static int MinimumExpectedVersion = (from fieldInfo in typeof(ExpectedVersion)
                    .GetFields(BindingFlags.Public | BindingFlags.Static | BindingFlags.FlattenHierarchy)
                where fieldInfo.IsLiteral
                      && !fieldInfo.IsInitOnly
                      && fieldInfo.FieldType == typeof(int)
                select (int) fieldInfo.GetRawConstantValue()).Min();

            public const string Authorization = "Authorization";
            public const string Allowed = "Allowed";
            public const string ExpectedVersion = "SSS-ExpectedVersion";
            public const string HeadPosition = "SSS-HeadPosition";
            public const string Location = "Location";
            public const string ETag = "ETag";
            public const string IfNoneMatch = "If-None-Match";
            public const string CacheControl = "Cache-Control";
            public const string ContentType = "Content-Type";
            public const string Accept = "Accept";
            public const string XRequestedWith = "X-Requested-With";

            public static class AccessControl
            {
                public const string AllowOrigin = "Access-Control-Allow-Origin";
                public const string AllowHeaders = "Access-Control-Allow-Headers";
                public const string AllowMethods = "Access-Control-Allow-Methods";
            }
        }

        public static class Relations
        {
            public const string StreamStorePrefix = "streamStore";
            public const string Curies = "curies";
            public const string Self = "self";
            public const string First = "first";
            public const string Previous = "previous";
            public const string Next = "next";
            public const string Last = "last";
            public const string Index = StreamStorePrefix + ":index";
            public const string Feed = StreamStorePrefix + ":feed";
            public const string Message = StreamStorePrefix + ":message";
            public const string Metadata = StreamStorePrefix + ":metadata";
            public const string AppendToStream = StreamStorePrefix + ":append";
            public const string DeleteStream = StreamStorePrefix + ":delete-stream";
            public const string DeleteMessage = StreamStorePrefix + ":delete-message";
            public const string Find = StreamStorePrefix + ":find";
            public const string Browse = StreamStorePrefix + ":feed-browser";
        }

        public static class Streams
        {
            public const string Stream = "streams";
            public const string All = "stream";
            public const string Metadata = "metadata";

            public static PathString AllStreamPath = new PathString($"/{All}");
            public static PathString StreamsPath = new PathString($"/{Stream}");
            public static PathString IndexPath = new PathString("/");
            public static PathString StreamBrowserPath = StreamsPath;
        }

        public static class ReadDirection
        {
            public const int Forwards = 1;
            public const int Backwards = -1;
        }

        public const int MaxCount = 20;
    }
}