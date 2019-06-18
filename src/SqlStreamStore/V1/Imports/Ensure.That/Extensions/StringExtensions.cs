#pragma warning disable CS1591 // Missing XML comment for publicly visible type or member

namespace SqlStreamStore.V1.Imports.Ensure.That.Extensions
{
    using System.Linq;

    internal static class StringExtensions
    {
        internal static string Inject(this string format, params object[] formattingArgs)
        {
            return string.Format(format, formattingArgs);
        }

        internal static string Inject(this string format, params string[] formattingArgs)
        {
            return string.Format(format, formattingArgs.Select(a => a as object).ToArray());
        }
    }
}