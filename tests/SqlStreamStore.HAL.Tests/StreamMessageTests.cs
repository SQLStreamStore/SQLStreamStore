namespace SqlStreamStore.HAL.Tests
{
    using System;
    using System.Net;
    using System.Net.Http.Headers;
    using System.Threading.Tasks;
    using Shouldly;
    using SqlStreamStore.Streams;
    using Xunit;
    using Xunit.Abstractions;

    public class StreamMessageTests : IDisposable
    {
        public StreamMessageTests(ITestOutputHelper output)
        {
            _fixture = new SqlStreamStoreHalMiddlewareFixture(output);
        }

        public void Dispose() => _fixture.Dispose();
        private readonly SqlStreamStoreHalMiddlewareFixture _fixture;
        private const string HeadOfStream = "streams/a-stream?d=b&m=20&p=-1&e=0";

        [Fact]
        public async Task read_single_message_stream()
        {
            var writeResult = await _fixture.WriteNMessages("a-stream", 1);

            using(var response = await _fixture.HttpClient.GetAsync($"/{Constants.Streams.Stream}/a-stream/0"))
            {
                response.StatusCode.ShouldBe(HttpStatusCode.OK);
                response.Headers.ETag.ShouldBe(new EntityTagHeaderValue($@"""{writeResult.CurrentVersion}"""));

                var resource = await response.AsHal();

                resource.ShouldLink(Links
                    .FromRequestMessage(response.RequestMessage)
                    .Index()
                    .Find()
                    .Add(Constants.Relations.Self, "streams/a-stream/0", "a-stream@0")
                    .Add(Constants.Relations.First, "streams/a-stream/0")
                    .Add(Constants.Relations.Next, "streams/a-stream/1")
                    .Add(Constants.Relations.Last, "streams/a-stream/-1")
                    .Add(Constants.Relations.Feed, HeadOfStream, "a-stream")
                    .Add(Constants.Relations.Message, "streams/a-stream/0", "a-stream@0"));
            }
        }

        [Fact]
        public async Task read_single_message_does_not_exist_stream()
        {
            using(var response = await _fixture.HttpClient.GetAsync($"/{Constants.Streams.Stream}/a-stream/0"))
            {
                response.StatusCode.ShouldBe(HttpStatusCode.NotFound);
                response.Headers.ETag.ShouldBeNull();

                var resource = await response.AsHal();

                resource.ShouldLink(Links
                    .FromRequestMessage(response.RequestMessage)
                    .Index()
                    .Find()
                    .Add(Constants.Relations.Self, "streams/a-stream/0", "a-stream@0")
                    .Add(Constants.Relations.First, "streams/a-stream/0")
                    .Add(Constants.Relations.Last, "streams/a-stream/-1")
                    .Add(Constants.Relations.Feed, HeadOfStream, "a-stream")
                    .Add(Constants.Relations.Message, "streams/a-stream/0", "a-stream@0"));
            }
        }

        [Fact]
        public async Task delete_single_message_by_version()
        {
            var writeResult = await _fixture.WriteNMessages("a-stream", 1);

            using(var response = await _fixture.HttpClient.DeleteAsync($"/{Constants.Streams.Stream}/a-stream/0"))
            {
                response.StatusCode.ShouldBe(HttpStatusCode.NoContent);
                response.Content.Headers.ContentLength.HasValue.ShouldBeTrue();
                response.Content.Headers.ContentLength.Value.ShouldBe(0);
            }

            using(var response = await _fixture.HttpClient.GetAsync($"/{Constants.Streams.Stream}/a-stream/0"))
            {
                response.StatusCode.ShouldBe(HttpStatusCode.NotFound);
                response.Headers.ETag.ShouldBeNull();
            }
        }
        
        [Fact]
        public async Task delete_single_message_by_message_id()
        {
            var writeResult = await _fixture.WriteNMessages("a-stream", 1);

            var page = await _fixture.StreamStore.ReadStreamForwards("a-stream", StreamVersion.Start, 1);

            var messageId = page.Messages[0].MessageId;

            using(var response = await _fixture.HttpClient.DeleteAsync($"/{Constants.Streams.Stream}/a-stream/{messageId}"))
            {
                response.StatusCode.ShouldBe(HttpStatusCode.NoContent);
                response.Content.Headers.ContentLength.HasValue.ShouldBeTrue();
                response.Content.Headers.ContentLength.Value.ShouldBe(0);
            }

            using(var response = await _fixture.HttpClient.GetAsync($"/{Constants.Streams.Stream}/a-stream/0"))
            {
                response.StatusCode.ShouldBe(HttpStatusCode.NotFound);
                response.Headers.ETag.ShouldBeNull();
            }
        }

    }
}