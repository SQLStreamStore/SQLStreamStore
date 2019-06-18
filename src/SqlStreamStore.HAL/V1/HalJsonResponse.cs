namespace SqlStreamStore.V1
{
    using System.IO;
    using System.Threading;
    using System.Threading.Tasks;
    using Halcyon.HAL;
    using Microsoft.AspNetCore.Http;
    using Newtonsoft.Json;
    using Newtonsoft.Json.Serialization;

    internal class HalJsonResponse : Response
    {
        private readonly HALResponse _body;

        private static readonly JsonSerializer s_serializer = JsonSerializer.Create(new JsonSerializerSettings
        {
            ContractResolver = new CamelCasePropertyNamesContractResolver(),
            TypeNameHandling = TypeNameHandling.None,
            NullValueHandling = NullValueHandling.Ignore
        });

        public HalJsonResponse(HALResponse body, int statusCode = 200)
            : base(statusCode, Constants.MediaTypes.HalJson)
        {
            _body = body;
        }

        public override async Task WriteBody(HttpResponse response, CancellationToken cancellationToken)
        {
            using(var writer = new JsonTextWriter(new StreamWriter(response.Body))
            {
                CloseOutput = false
            })
            {
                await _body.ToJObject(s_serializer).WriteToAsync(writer, cancellationToken);
                await writer.FlushAsync(cancellationToken);
            }
        }
    }
}