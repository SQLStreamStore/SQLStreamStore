namespace SqlStreamStore.MsSqlScripts
{
    using System;
    using System.Collections.Generic;
    using System.Linq;

    public static class DatabaseSchemaExtensions
    {

        public static string[] CompareTo(this string[] source, string[] target,
            string type, string sourcename, string targetName)
        {
            var missingInTarget = source
                .Where(item => !target.Contains(item, StringComparer.OrdinalIgnoreCase))
                .Select(x => $"{type} '{x}' is missing from {targetName} but is in {sourcename}");

            var missingInSource = target
                .Where(item => !source.Contains(item, StringComparer.OrdinalIgnoreCase))
                .Select(x => $"{type} '{x}' is missing from {sourcename} but is in {targetName}");

            return missingInTarget
                .Union(missingInSource)
                .ToArray();
        }
        public static string[] CompareTo<T>(this SortedDictionary<string, T> source, SortedDictionary<string, T> target,
            string type, string sourcename, string targetName, DatabaseSchema.CompareItem<T> compareItem)
        {
            var missingInTarget = source.Keys
                .Where(x => !target.ContainsKey(x))
                .Select(x => $"{type} '{x}' is missing from {targetName} but is in {sourcename}");

            var missingInSource = target.Keys
                .Where(x => !source.ContainsKey(x))
                .Select(x => $"{type} '{x}' is missing from {sourcename} but is in {targetName}");

            var comparissonIssues = source
                .Where(x => target.ContainsKey(x.Key))
                .SelectMany(x => compareItem(x.Value, target[x.Key], x.Key));

            return missingInTarget
                .Union(missingInSource)
                .Union(comparissonIssues)
                .ToArray();
        }

        public static string ToStringJoined<T>(this IEnumerable<T> items, string separator = ",")
        {
            if (items == null || !items.Any()) return null;
            return string.Join(separator, items.Select(x => x.ToString()));
        }

        public static string[] Sort(this string[] items)
        {
            if (items == null)
                return new string[0];

            return items
                .OrderBy(x => x)
                .ToArray();
        }
    }
}