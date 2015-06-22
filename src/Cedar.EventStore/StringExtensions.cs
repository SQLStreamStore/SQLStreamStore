namespace Cedar.EventStore
{
    public static class StringExtensions
    {
        public static string FormatWith(this string s, params object[] args)
        {
            return string.Format(s, args);
        }
    }
}