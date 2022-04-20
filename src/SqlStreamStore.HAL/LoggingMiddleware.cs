namespace SqlStreamStore.HAL
{
    using System;
    using System.Threading.Tasks;
    using Microsoft.AspNetCore.Builder;
    using Microsoft.AspNetCore.Http;
    using Microsoft.Extensions.DependencyInjection;
    using Microsoft.Extensions.Logging;

    internal static class LoggingMiddleware
    {
        public static IApplicationBuilder UseMiddlewareLogging(this IApplicationBuilder builder, Type middlewareType)
        {
            var log = builder.ApplicationServices.GetService<ILoggerFactory>().CreateLogger(middlewareType);
            
            Task MiddlewareLogging(HttpContext context, Func<Task> next)
            {
                log.LogDebug("Middleware: {middleware}; Connection Id: {connectionId}", middlewareType, context.Connection.Id);
                return next();
            }

            return builder.Use(MiddlewareLogging);
        }
    }
}