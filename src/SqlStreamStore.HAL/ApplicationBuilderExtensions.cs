namespace SqlStreamStore
{
    using System;
    using System.Net.Http;
    using Microsoft.AspNetCore.Builder;

    internal static class ApplicationBuilderExtensions
    {
        public static IApplicationBuilder MapWhen(
            this IApplicationBuilder builder,
            HttpMethod method,
            Action<IApplicationBuilder> configure) => builder.MapWhen(
            context => string.Equals(
                context.Request.Method,
                method.Method,
                StringComparison.OrdinalIgnoreCase),
            configure);
    }
}