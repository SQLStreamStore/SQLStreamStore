namespace SqlStreamStore.V1
{
    using System;
    using System.Linq;
    using System.Net.Http.Headers;
    using System.Threading.Tasks;
    using Microsoft.AspNetCore.Http;
    using SqlStreamStore.V1.Streams;

    internal static class HttpContextExtensions
    {
        private static readonly string[] s_NotModifiedRequiredHeaders =
        {
            "cache-control",
            "content-location",
            "date",
            "etag",
            "expires",
            "vary"
        };

        public static Task WriteResponse(this HttpContext context, Response response)
            => context.Request.IfNoneMatch(response)
                ? WriteNotModifiedResponse(context, response)
                : WriteResponseInternal(context, response);

        private static bool IfNoneMatch(this HttpRequest request, Response response)
        {
            if(!request.Headers.TryGetValue(Constants.Headers.IfNoneMatch, out var ifNoneMatch)
               || !response.Headers.TryGetValue(Constants.Headers.ETag, out var eTags)
               || eTags.Length == 0)
            {
                return false;
            }

            foreach(var candidate in ifNoneMatch)
            {
                if(string.Equals(eTags[0], candidate, StringComparison.Ordinal))
                {
                    return true;
                }
            }

            return false;
        }

        private static Task WriteNotModifiedResponse(HttpContext context, Response response)
        {
            context.Response.StatusCode = 304;
            foreach(var header in s_NotModifiedRequiredHeaders.Where(response.Headers.Keys.Contains))
            {
                context.Response.Headers.AppendCommaSeparatedValues(header, response.Headers[header]);
            }

            return Task.CompletedTask;
        }

        private static Task WriteResponseInternal(HttpContext context, Response response)
        {
            context.Response.StatusCode = response.StatusCode;

            foreach(var header in response.Headers)
            {
                context.Response.Headers.AppendCommaSeparatedValues(header.Key, header.Value);
            }

            return response.WriteBody(context.Response, context.RequestAborted);
        }

        public static int GetExpectedVersion(this HttpRequest request)
            => int.TryParse(
                request.Headers[Constants.Headers.ExpectedVersion],
                out var expectedVersion)
                ? expectedVersion
                : ExpectedVersion.Any;

        public static string[] GetAcceptHeaders(this HttpRequest contextRequest)
            => Array.ConvertAll(
                contextRequest.Headers
                    .GetCommaSeparatedValues("Accept"),
                value => MediaTypeWithQualityHeaderValue.TryParse(value, out var header)
                    ? header.MediaType
                    : null);
    }
}