using System;
using System.Linq;
using System.Net.Http;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.Primitives;

namespace SqlStreamStore
{
    internal static class AllowedMethodsMiddleware
    {
        public static IApplicationBuilder UseAllowedMethods(this IApplicationBuilder builder, IResource resource)
        {
            var allowed = ResourceMethods.Discover(resource);

            var allowedMethodsHeaderValue = allowed.Distinct().Aggregate(
                StringValues.Empty,
                (previous, method) => StringValues.Concat(previous, method.Method));

            var allowedHeadersHeaderValue = new StringValues(new[]
            {
                Constants.Headers.ContentType,
                Constants.Headers.XRequestedWith,
                Constants.Headers.Authorization
            });
            
            Task Options(HttpContext context, Func<Task> next)
            {
                context.Response.Headers.AppendCommaSeparatedValues(
                    Constants.Headers.AccessControl.AllowMethods,
                    allowedMethodsHeaderValue);
                context.Response.Headers.AppendCommaSeparatedValues(
                    Constants.Headers.AccessControl.AllowHeaders,
                    allowedHeadersHeaderValue);
                context.Response.Headers.AppendCommaSeparatedValues(
                    Constants.Headers.AccessControl.AllowOrigin,
                    "*");
                
                return Task.CompletedTask;
            }

            Task AllowedMethods(HttpContext context, Func<Task> next)
            {
                if(!allowed.Contains(new HttpMethod(context.Request.Method)))
                {
                    context.Response.StatusCode = 405;
                    context.Response.Headers.Add(Constants.Headers.Allowed, allowedMethodsHeaderValue);
                    return Task.CompletedTask;
                }

                return next();
            }

            return builder
                .MapWhen(HttpMethod.Options, inner => inner.Use(Options))
                .Use(AllowedMethods);
        }
    }
}