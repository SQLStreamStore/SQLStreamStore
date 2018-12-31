namespace SqlStreamStore.HAL.DevServer
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Net.Http;
    using System.Net.Http.Headers;
    using System.Net.WebSockets;
    using System.Threading;
    using System.Threading.Tasks;
    using Microsoft.AspNetCore.Builder;
    using Microsoft.AspNetCore.Http;
    using Microsoft.Extensions.Primitives;
    using MidFunc = System.Func<
        Microsoft.AspNetCore.Http.HttpContext,
        System.Func<System.Threading.Tasks.Task>,
        System.Threading.Tasks.Task
    >;

    internal static class SqlStreamStoreBrowserExtensions
    {
        public static IApplicationBuilder UseSqlStreamStoreBrowser(
            this IApplicationBuilder builder)
        {
            var httpClient = new HttpClient();

            return builder
                .UseWebSockets()
                .Use(ForwardWebsockets(httpClient))
                .Use(ForwardAcceptHtml(httpClient))
                .Use(ForwardStaticFiles(httpClient));
        }

        private static MidFunc ForwardWebsockets(HttpClient httpClient) => (context, next)
            => context.WebSockets.IsWebSocketRequest
                ? ForwardWebsocketToClientDevServer(context)
                : (context.Request.Path.StartsWithSegments(new PathString("/sockjs-node"))
                    ? ForwardToClientDevServer(httpClient, context)
                    : next());


        private static MidFunc ForwardStaticFiles(HttpClient httpClient) => (context, next)
            => context.Request.Path.StartsWithSegments(new PathString("/static"))
                || context.Request.Path.StartsWithSegments("/__webpack_dev_server__")
                ? ForwardToClientDevServer(
                    httpClient,
                    context)
                : next();

        private static MidFunc ForwardAcceptHtml(HttpClient httpClient) => (context, next)
            => GetAcceptHeaders(context.Request)
                .Any(header => header == "text/html")
                ? ForwardToClientDevServer(
                    httpClient,
                    context,
                    context.Request.PathBase.ToUriComponent())
                : next();

        private static Task ForwardToClientDevServer(
            HttpClient httpClient,
            HttpContext context)
            => ForwardToClientDevServer(httpClient, context, context.Request.Path);

        private static async Task ForwardToClientDevServer(
            HttpClient httpClient,
            HttpContext context,
            PathString path)
        {
            using(var request = BuildRequest(context, path))
            using(var response = await httpClient.SendAsync(request))
            using(var stream = await response.Content.ReadAsStreamAsync())
            {
                context.Response.StatusCode = (int) response.StatusCode;

                var headers = from header in response.Headers.Concat(
                        response.Content?.Headers
                        ?? Enumerable.Empty<KeyValuePair<string, IEnumerable<string>>>())
                    where !"transfer-encoding".Equals(header.Key, StringComparison.InvariantCultureIgnoreCase)
                    select header;

                foreach(var header in headers)
                {
                    context.Response.Headers.Add(header.Key, new StringValues(header.Value.ToArray()));
                }

                await stream.CopyToAsync(context.Response.Body, 8196, context.RequestAborted);

                await context.Response.Body.FlushAsync(context.RequestAborted);
            }
        }

        private static HttpRequestMessage BuildRequest(HttpContext context, PathString path)
        {
            var request = new HttpRequestMessage(
                new HttpMethod(context.Request.Method),
                new UriBuilder
                {
                    Port = 3000,
                    Path = path.ToUriComponent(),
                    Query = context.Request.QueryString.ToUriComponent()
                }.Uri);

            foreach(var header in context.Request.Headers)
            {
                var values = header.Value.ToArray();

                request.Headers.TryAddWithoutValidation(header.Key, values);
                request.Content?.Headers.TryAddWithoutValidation(header.Key, values);
            }

            return request;
        }

        private static string[] GetAcceptHeaders(HttpRequest contextRequest)
            => Array.ConvertAll(
                contextRequest.Headers.GetCommaSeparatedValues("Accept"),
                value => MediaTypeWithQualityHeaderValue.TryParse(value, out var header)
                    ? header.MediaType
                    : null);

        static async Task ForwardWebsocketToClientDevServer(HttpContext context)
        {
            var socket = await context.WebSockets.AcceptWebSocketAsync();

            using(var forwarder = new WebsocketForwarder(socket))
            {
                await forwarder.SendAndReceive(context.RequestAborted);
            }
        }

        private class WebsocketForwarder : IDisposable
        {
            private readonly WebSocket _socket;
            private readonly ClientWebSocket _client;

            public WebsocketForwarder(WebSocket socket)
            {
                _socket = socket;
                _client = new ClientWebSocket();
            }

            public async Task SendAndReceive(CancellationToken ct)
            {
                await _client.ConnectAsync(new UriBuilder
                    {
                        Port = 3000
                    }.Uri,
                    ct);

                await Task.WhenAll(Send(ct), Receive(ct));
            }

            private async Task Send(CancellationToken ct)
            {
                var receiveBuffer = new byte[4096];

                while(_socket.State == WebSocketState.Open)
                {
                    var buffer = new ArraySegment<byte>(receiveBuffer);

                    var result = await _socket.ReceiveAsync(
                        buffer,
                        ct);

                    if(result.MessageType == WebSocketMessageType.Close)
                    {
                        await _client.CloseAsync(
                            result.CloseStatus ?? WebSocketCloseStatus.Empty,
                            result.CloseStatusDescription,
                            ct);

                        return;
                    }

                    await _client.SendAsync(buffer, result.MessageType, result.EndOfMessage, ct);
                }
            }

            private async Task Receive(CancellationToken ct)
            {
                var sendBuffer = new byte[4096];

                while(_socket.State == WebSocketState.Open)
                {
                    var buffer = new ArraySegment<byte>(sendBuffer);

                    var result = await _client.ReceiveAsync(
                        buffer,
                        ct);

                    if(result.MessageType == WebSocketMessageType.Close)
                    {
                        await _socket.CloseAsync(
                            result.CloseStatus ?? WebSocketCloseStatus.Empty,
                            result.CloseStatusDescription,
                            ct);

                        return;
                    }

                    await _socket.SendAsync(buffer, result.MessageType, result.EndOfMessage, ct);
                }
            }

            public void Dispose() => _client?.Dispose();
        }
    }
}