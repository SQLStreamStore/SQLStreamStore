namespace SqlStreamStore.AllStreamMessage
{
    using System;
    using System.Threading;
    using System.Threading.Tasks;
    using Halcyon.HAL;
    using SqlStreamStore.Streams;

    internal class AllStreamMessageResource : IResource
    {
        private readonly IStreamStore _streamStore;

        public SchemaSet Schema { get; }

        public AllStreamMessageResource(IStreamStore streamStore)
        {
            if(streamStore == null)
                throw new ArgumentNullException(nameof(streamStore));
            _streamStore = streamStore;
        }

        public async Task<Response> Get(
            ReadAllStreamMessageOperation operation,
            CancellationToken cancellationToken)
        {
            var message = await operation.Invoke(_streamStore, cancellationToken);

            var links = Links
                .FromOperation(operation)
                .Index()
                .Find()
                .Browse()
                .Add(
                    Constants.Relations.Message,
                    LinkFormatter.AllStreamMessageByPosition(message.Position),
                    $"{message.StreamId}@{message.StreamVersion}")
                .Self()
                .Add(
                    Constants.Relations.Feed,
                    LinkFormatter.ReadAllBackwards(
                        Position.End,
                        Constants.MaxCount,
                        false));

            if(message.MessageId == Guid.Empty)
            {
                return new HalJsonResponse(
                    new HALResponse(null)
                        .AddLinks(links),
                    404);
            }

            var payload = await message.GetJsonData(cancellationToken);

            return new HalJsonResponse(new StreamMessageHALResponse(message, payload).AddLinks(links));
        }
    }
}