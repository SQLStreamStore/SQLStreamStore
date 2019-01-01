namespace SqlStreamStore.HAL.Tests
{
    using System;
    using System.Net;
    using System.Threading.Tasks;
    using Microsoft.AspNetCore.Http;
    using Shouldly;
    using Xunit;

    public class AllStreamMessageTests : IDisposable
    {
        public AllStreamMessageTests()
        {
            _fixture = new SqlStreamStoreHalMiddlewareFixture();
        }

        public void Dispose() => _fixture.Dispose();
        private readonly SqlStreamStoreHalMiddlewareFixture _fixture;
        private const string HeadOfAll = "stream?d=b&m=20&p=-1&e=0";

        [Fact]
        public async Task read_single_message_all_stream()
        {
            // position of event in all stream would be helpful here
            var writeResult = await _fixture.WriteNMessages("a-stream", 1);

            using(var response = await _fixture.HttpClient.GetAsync("/stream/0"))
            {
                response.StatusCode.ShouldBe(HttpStatusCode.OK);

                var resource = await response.AsHal();

                resource.ShouldLink(
                    Links
                        .FromRequestMessage(response.RequestMessage)
                        .Find()
                        .Index()
                        .AddSelf(Constants.Relations.Message, "stream/0", "a-stream@0")
                        .Add(Constants.Relations.Feed, HeadOfAll));
            }
        }

        [Fact]
        public async Task read_single_message_does_not_exist_all_stream()
        {
            using(var response = await _fixture.HttpClient.GetAsync("/stream/0"))
            {
                response.StatusCode.ShouldBe(HttpStatusCode.NotFound);

                var resource = await response.AsHal();

                resource.ShouldLink(Links
                    .FromPath(new PathString("/stream/0"))
                    .AddSelf(Constants.Relations.Message, "stream/0", "@0")
                    .Add(Constants.Relations.Feed, HeadOfAll));
            }
        }
    }
}