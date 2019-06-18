namespace SqlStreamStore
{
    using System;
    using System.Collections.Generic;
    using System.Threading;
    using System.Threading.Tasks;
    using Microsoft.AspNetCore.Http;

    internal abstract class Response
    {
        public int StatusCode { get; }
        public IDictionary<string, string[]> Headers { get; }

        protected Response(int statusCode, string mediaType = null)
        {
            StatusCode = statusCode;
            Headers = new Dictionary<string, string[]>(StringComparer.OrdinalIgnoreCase)
            {
                [Constants.Headers.ContentType] = new[] { mediaType }
            };
        }

        public abstract Task WriteBody(HttpResponse response, CancellationToken cancellationToken);
    }
}