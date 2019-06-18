namespace SqlStreamStore.V1
{
    using System.IO;
    using System.Net.Http.Headers;
    using System.Threading;
    using System.Threading.Tasks;
    using Microsoft.AspNetCore.Http;

    internal class MarkdownResponse : Response
    {
        private static readonly string s_TextMarkdown
            = new MediaTypeHeaderValue(Constants.MediaTypes.TextMarkdown)
            {
                CharSet = "utf-8"
            }.ToString();

        private readonly Stream _body;

        public MarkdownResponse(Stream body)
            : base(body == null ? 404 : 200, s_TextMarkdown)
        {
            _body = body;
        }

        public override Task WriteBody(HttpResponse response, CancellationToken cancellationToken)
            => _body?.CopyToAsync(response.Body, 8192, cancellationToken) ?? Task.CompletedTask;
    }
}