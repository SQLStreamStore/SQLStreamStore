namespace SqlStreamStore
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Net;
    using System.Net.Http;
    using System.Threading;
    using System.Threading.Tasks;

    /// <summary>
    /// A delegating handler that handles HTTP redirects (301, 302, 303, 307, and 308).
    /// https://gist.github.com/joelverhagen/3be85bc0d5733756befa#file-redirectinghandler-cs
    /// </summary>
    internal class RedirectingHandler : DelegatingHandler
    {
        /// <summary>
        /// The property key used to access the list of responses.
        /// </summary>
        public const string HistoryPropertyKey = "Knapcode.Http.Handlers.RedirectingHandler.ResponseHistory";

        private static readonly ISet<HttpStatusCode> s_redirectStatusCodes = new HashSet<HttpStatusCode>(new[]
        {
            HttpStatusCode.MovedPermanently,
            HttpStatusCode.Found,
            HttpStatusCode.SeeOther,
            HttpStatusCode.PermanentRedirect,
        });

        private static readonly ISet<HttpStatusCode> s_keepRequestBodyRedirectStatusCodes = new HashSet<HttpStatusCode>(
            new[]
            {
                HttpStatusCode.TemporaryRedirect,
                HttpStatusCode.PermanentRedirect,
            });


        /// <summary>
        /// Initializes a new instance of the <see cref="RedirectingHandler"/> class.
        /// </summary>
        public RedirectingHandler()
        {
            AllowAutoRedirect = true;
            MaxAutomaticRedirections = 50;
            DisableInnerAutoRedirect = true;
            DownloadContentOnRedirect = true;
            KeepResponseHistory = true;
        }

        /// <summary>
        /// Gets or sets a value that indicates whether the handler should follow redirection responses.
        /// </summary>
        public bool AllowAutoRedirect { get; set; }

        /// <summary>
        /// Gets or sets the maximum number of redirects that the handler follows.
        /// </summary>
        public int MaxAutomaticRedirections { get; set; }

        /// <summary>
        /// Gets or sets a value indicating whether the response body should be downloaded before each redirection.
        /// </summary>
        public bool DownloadContentOnRedirect { get; set; }

        /// <summary>
        /// Gets or sets a value indicating inner redirections on <see cref="HttpClientHandler"/> and <see cref="RedirectingHandler"/> should be disabled.
        /// </summary>
        public bool DisableInnerAutoRedirect { get; set; }

        /// <summary>
        /// Gets or sets a value indicating whether the response history should be saved to the <see cref="HttpResponseMessage.RequestMessage"/> properties with the key of <see cref="HistoryPropertyKey"/>.
        /// </summary>
        public bool KeepResponseHistory { get; set; }

        protected override async Task<HttpResponseMessage> SendAsync(
            HttpRequestMessage request,
            CancellationToken cancellationToken)
        {
            if(DisableInnerAutoRedirect)
            {
                // find the inner-most handler
                HttpMessageHandler innerHandler = InnerHandler;
                while(innerHandler is DelegatingHandler)
                {
                    if(innerHandler is RedirectingHandler redirectingHandler)
                    {
                        redirectingHandler.AllowAutoRedirect = false;
                    }

                    innerHandler = ((DelegatingHandler) innerHandler).InnerHandler;
                }

                if(innerHandler is HttpClientHandler httpClientHandler)
                {
                    httpClientHandler.AllowAutoRedirect = false;
                }
            }

            // buffer the request body, to allow re-use in redirects
            HttpContent requestBody = null;
            if(AllowAutoRedirect && request.Content != null)
            {
                var buffer = await request.Content.ReadAsByteArrayAsync();
                requestBody = new ByteArrayContent(buffer);
                foreach(var (key, value) in request.Content.Headers)
                {
                    requestBody.Headers.Add(key, value);
                }
            }

            // make a copy of the request headers
            var requestHeaders = request
                .Headers
                .Select(p => new KeyValuePair<string, string[]>(p.Key, p.Value.ToArray()))
                .ToArray();

            // send the initial request
            var response = await base.SendAsync(request, cancellationToken);
            var responses = new List<HttpResponseMessage>();

            var redirectCount = 0;
            while(AllowAutoRedirect && redirectCount < MaxAutomaticRedirections
                                    && TryGetRedirectLocation(response, out var locationString))
            {
                if(DownloadContentOnRedirect && response.Content != null)
                {
                    await response.Content.ReadAsByteArrayAsync();
                }

                Uri previousRequestUri = response.RequestMessage.RequestUri;

                // Credit where credit is due: https://github.com/kennethreitz/requests/blob/master/requests/sessions.py
                // allow redirection without a scheme
                if(locationString.StartsWith("//"))
                {
                    locationString = previousRequestUri.Scheme + ":" + locationString;
                }

                var nextRequestUri = new Uri(locationString, UriKind.RelativeOrAbsolute);

                // allow relative redirects
                if(!nextRequestUri.IsAbsoluteUri)
                {
                    nextRequestUri = new Uri(previousRequestUri, nextRequestUri);
                }

                // override previous method
                HttpMethod nextMethod = response.RequestMessage.Method;
                if(response.StatusCode == HttpStatusCode.Moved && nextMethod == HttpMethod.Post
                   || response.StatusCode == HttpStatusCode.Found && nextMethod != HttpMethod.Head
                   || response.StatusCode == HttpStatusCode.SeeOther && nextMethod != HttpMethod.Head)
                {
                    nextMethod = HttpMethod.Get;
                    requestBody = null;
                }

                if(!s_keepRequestBodyRedirectStatusCodes.Contains(response.StatusCode))
                {
                    requestBody = null;
                }

                // build the next request
                var nextRequest = new HttpRequestMessage(nextMethod, nextRequestUri)
                {
                    Content = requestBody,
                    Version = request.Version
                };

                foreach(var header in requestHeaders)
                {
                    nextRequest.Headers.Add(header.Key, header.Value);
                }

                foreach(var pair in request.Properties)
                {
                    nextRequest.Properties.Add(pair.Key, pair.Value);
                }

                // keep a history all responses
                if(KeepResponseHistory)
                {
                    responses.Add(response);
                }

                // send the next request
                response = await base.SendAsync(nextRequest, cancellationToken);

                request = response.RequestMessage;
                redirectCount++;
            }

            // save the history to the request message properties
            if(KeepResponseHistory && response.RequestMessage != null)
            {
                responses.Add(response);
                response.RequestMessage.Properties.Add(HistoryPropertyKey, responses);
            }

            return response;
        }

        private static bool TryGetRedirectLocation(HttpResponseMessage response, out string location)
        {
            if(s_redirectStatusCodes.Contains(response.StatusCode)
               && response.Headers.TryGetValues("Location", out var locations)
               && (locations = locations.ToArray()).Count() == 1
               && !string.IsNullOrWhiteSpace(locations.First()))
            {
                location = locations.First().Trim();
                return true;
            }

            location = null;
            return false;
        }
    }
}