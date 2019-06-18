namespace SqlStreamStore.V1
{
    using System.Threading;
    using System.Threading.Tasks;
    using Microsoft.AspNetCore.Http;

    internal class TemporaryRedirectResponse : Response
    {
        public TemporaryRedirectResponse(string location)
            : base(307)
        {
            Headers[Constants.Headers.Location] = new[] { location };
        }

        public override Task WriteBody(HttpResponse response, CancellationToken cancellationToken)
            => Task.CompletedTask;
    }
}