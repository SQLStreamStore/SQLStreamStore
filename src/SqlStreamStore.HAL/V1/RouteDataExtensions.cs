namespace SqlStreamStore.V1
{
    using System;
    using Microsoft.AspNetCore.Routing;

    internal static class RouteDataExtensions
    {
        public static string GetStreamId(this RouteData routeData)
            => routeData.Values["streamId"]?.ToString().Unescape();

        public static long GetPosition(this RouteData routeData)
            => long.TryParse(routeData.Values["position"]?.ToString(), out var value)
                ? value
                : default;

        public static int GetStreamVersion(this RouteData routeData)
            => int.TryParse(routeData.Values["p"]?.ToString(), out var value)
                ? value
                : default;

        public static Guid GetMessageId(this RouteData routeData)
            => Guid.TryParse(routeData.Values["p"]?.ToString(), out var value)
                ? value
                : default;

        public static string GetDoc(this RouteData routeData)
            => routeData.Values["doc"]?.ToString();

        private static string Unescape(this string s)
            => Uri.UnescapeDataString(s);
    }
}