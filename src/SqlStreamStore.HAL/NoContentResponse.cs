namespace SqlStreamStore.HAL
{
    using System.Threading;
    using System.Threading.Tasks;
    using Microsoft.AspNetCore.Http;

    internal class NoContentResponse : Response
    {
        public static readonly NoContentResponse Instance = new NoContentResponse();

        private NoContentResponse()
            : base(204)
        { }

        public override Task WriteBody(HttpResponse response, CancellationToken cancellationToken)
            => Task.CompletedTask;
    }
}