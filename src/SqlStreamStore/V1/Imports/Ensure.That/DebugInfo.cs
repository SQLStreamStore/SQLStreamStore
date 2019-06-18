#pragma warning disable CS1591 // Missing XML comment for publicly visible type or member

namespace SqlStreamStore.V1.Imports.Ensure.That
{
    public static class DebugInfo
    {
        public static string Target()
        {
#if NET
            return "NET";
#elif DOTNETCORE
            return "DOTNETCORE";
#else
            return "UNKNOWN";
#endif
        }
    }
}