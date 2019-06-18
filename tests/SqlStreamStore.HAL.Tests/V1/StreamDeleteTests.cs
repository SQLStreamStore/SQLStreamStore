namespace SqlStreamStore.V1
{
    using System;
    using System.Net;
    using System.Net.Http;
    using System.Net.Http.Headers;
    using System.Threading.Tasks;
    using Shouldly;
    using SqlStreamStore.V1.Streams;
    using Xunit;
    using Xunit.Abstractions;

    public class StreamDeleteTests : IDisposable
    {
        private const string StreamId = "a-stream";
        private readonly SqlStreamStoreHalMiddlewareFixture _fixture;

        public StreamDeleteTests(ITestOutputHelper output)
        {
            _fixture = new SqlStreamStoreHalMiddlewareFixture(output);
        }

        [Theory, InlineData(ExpectedVersion.Any), InlineData(0), InlineData(null)]
        public async Task expected_version(int? expectedVersion)
        {
            await _fixture.WriteNMessages(StreamId, 1);

            var request = new HttpRequestMessage(HttpMethod.Delete, $"/{Constants.Paths.Streams}/{StreamId}");
            
            if(expectedVersion.HasValue)
            {
                request.Headers.Add(Constants.Headers.ExpectedVersion, $"{expectedVersion}");
            }
            
            using(var response = await _fixture.HttpClient.SendAsync(request))
            {
                response.StatusCode.ShouldBe(HttpStatusCode.NoContent);
                response.Content.Headers.ContentLength.HasValue.ShouldBeTrue();
                response.Content.Headers.ContentLength.Value.ShouldBe(0);
            }

            var page = await _fixture.StreamStore.ReadStreamForwards(StreamId, 0, 1);
            
            page.Status.ShouldBe(PageReadStatus.StreamNotFound);
        }

        [Theory, InlineData(ExpectedVersion.NoStream), InlineData(2)]
        public async Task wrong_expected_version(int expectedVersion)
        {
            await _fixture.WriteNMessages(StreamId, 1);
            var request = new HttpRequestMessage(HttpMethod.Delete, $"/{Constants.Paths.Streams}/{StreamId}")
            {
                Headers =
                {
                    { Constants.Headers.ExpectedVersion, $"{expectedVersion}" }
                }
            };
            
            using(var response = await _fixture.HttpClient.SendAsync(request))
            {
                response.StatusCode.ShouldBe(HttpStatusCode.Conflict);
                response.Content.Headers.ContentType.ShouldBe(new MediaTypeHeaderValue(
                    Constants.MediaTypes.HalJson));
            }

            var page = await _fixture.StreamStore.ReadStreamForwards(StreamId, 0, 1);
            
            page.Status.ShouldBe(PageReadStatus.Success);

        }

        public void Dispose() => _fixture.Dispose();
    }
}