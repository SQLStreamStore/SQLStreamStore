namespace SqlStreamStore.Imports.Ensure.That
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