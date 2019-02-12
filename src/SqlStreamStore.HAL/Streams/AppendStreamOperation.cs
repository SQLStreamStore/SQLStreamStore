namespace SqlStreamStore.HAL.Streams
{
    using System;
    using System.IO;
    using System.Linq;
    using System.Threading;
    using System.Threading.Tasks;
    using Microsoft.AspNetCore.Http;
    using Newtonsoft.Json;
    using Newtonsoft.Json.Linq;
    using SqlStreamStore.Streams;

    internal class AppendStreamOperation : IStreamStoreOperation<AppendResult>
    {
        public static async Task<AppendStreamOperation> Create(HttpRequest request, CancellationToken ct)
        {
            using(var reader = new JsonTextReader(new StreamReader(request.Body))
            {
                CloseInput = false
            })
            {
                var body = await JToken.LoadAsync(reader, ct);

                switch(body)
                {
                    case JArray json:
                        return new AppendStreamOperation(request, json);
                    case JObject json:
                        return new AppendStreamOperation(request, json);
                    default:
                        throw new InvalidAppendRequestException("Invalid json detected.");
                }
            }
        }

        private AppendStreamOperation(HttpRequest request)
        {
            StreamId = request.Path.Value.Remove(0, 2 + Constants.Streams.Stream.Length);

            ExpectedVersion = request.GetExpectedVersion();
        }

        private AppendStreamOperation(HttpRequest request, JArray body)
            : this(request)
        {
            Path = request.Path;
            NewStreamMessages = body.Select(ParseNewStreamMessage).ToArray();
        }

        private AppendStreamOperation(HttpRequest request, JObject body)
            : this(request, new JArray { body })
        { }

        private static NewStreamMessageDto ParseNewStreamMessage(JToken newStreamMessage, int index)
        {
            if(!Guid.TryParse(newStreamMessage.Value<string>("messageId"), out var messageId))
            {
                throw new InvalidAppendRequestException(
                    $"'{nameof(messageId)}' at index {index} was improperly formatted.");
            }

            if(messageId == Guid.Empty)
            {
                throw new InvalidAppendRequestException($"'{nameof(messageId)}' at index {index} was empty.");
            }

            var type = newStreamMessage.Value<string>("type");

            if(type == null)
            {
                throw new InvalidAppendRequestException($"'{nameof(type)}' at index {index} was not set.");
            }

            return new NewStreamMessageDto
            {
                MessageId = messageId,
                Type = type,
                JsonData = newStreamMessage.Value<JObject>("jsonData"),
                JsonMetadata = newStreamMessage.Value<JObject>("jsonMetadata")
            };
        }

        public string StreamId { get; }
        public int ExpectedVersion { get; }
        public NewStreamMessageDto[] NewStreamMessages { get; }
        public PathString Path { get; }

        public Task<AppendResult> Invoke(IStreamStore streamStore, CancellationToken ct)
            => streamStore.AppendToStream(
                StreamId,
                ExpectedVersion,
                Array.ConvertAll(NewStreamMessages, dto => dto.ToNewStreamMessage()),
                ct);
    }
}