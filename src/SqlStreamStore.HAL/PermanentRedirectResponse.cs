namespace SqlStreamStore.HAL
{
    using System.Threading;
    using System.Threading.Tasks;
    using Microsoft.AspNetCore.Http;

    internal class PermanentRedirectResponse : Response
    {
        public PermanentRedirectResponse(string location)
            : base(308)
        {
            Headers[Constants.Headers.Location] = new[] { location };
        }

        public override Task WriteBody(HttpResponse response, CancellationToken cancellationToken)
            => Task.CompletedTask;
    }
}