namespace SqlStreamStore
{
    using System;
    using System.Threading.Tasks;
    using Microsoft.AspNetCore.Builder;
    using Microsoft.AspNetCore.Http;
    using SqlStreamStore.Logging;

    internal static class LoggingMiddleware
    {
        public static IApplicationBuilder UseMiddlewareLogging(this IApplicationBuilder builder, Type middlewareType)
        {
            var log = LogProvider.GetLogger(middlewareType);
            
            Task MiddlewareLogging(HttpContext context, Func<Task> next)
            {
                log.Debug("Middleware: {middleware}; Connection Id: {connectionId}", middlewareType, context.Connection.Id);
                return next();
            }

            return builder.Use(MiddlewareLogging);
        }
    }
}