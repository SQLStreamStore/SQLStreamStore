namespace SqlStreamStore.HAL
{
    using System;
    using System.Threading.Tasks;
    using Microsoft.AspNetCore.Builder;
    using Microsoft.AspNetCore.Http;
    using SqlStreamStore.HAL.Logging;

    internal static class LoggingMiddleware
    {
        public static IApplicationBuilder UseMiddlewareLogging(this IApplicationBuilder builder, Type middlewareType)
        {
            var log = LogProvider.GetLogger(middlewareType);
            
            Task MiddlewareLogging(HttpContext context, Func<Task> next)
            {
                log.Info($"Middleware Used: {middlewareType.FullName}; Request: {context.TraceIdentifier}");
                return next();
            }

            return builder.Use(MiddlewareLogging);
        }
    }
}