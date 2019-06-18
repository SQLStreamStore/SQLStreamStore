namespace SqlStreamStore.V1
{
    using System;
    using System.Net;
    using System.Threading.Tasks;
    using Microsoft.AspNetCore.Http;
    using Shouldly;
    using SqlStreamStore.V1.Streams;
    using Xunit;
    using Xunit.Abstractions;

    public class AllStreamMessageTests : IDisposable
    {
        public AllStreamMessageTests(ITestOutputHelper output)
        {
            _fixture = new SqlStreamStoreHalMiddlewareFixture(output);
        }

        public void Dispose() => _fixture.Dispose();
        private readonly SqlStreamStoreHalMiddlewareFixture _fixture;
        private static readonly string HeadOfAll = LinkFormatter.ReadAllBackwards(Position.End, 20, false);

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
                        .AddSelf(
                            Constants.Relations.Message,
                            LinkFormatter.AllStreamMessageByPosition(0),
                            "a-stream@0")
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