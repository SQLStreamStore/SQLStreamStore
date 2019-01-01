namespace SqlStreamStore.HAL.Tests
{
    using System;
    using System.Collections.Generic;
    using System.Net.Http;
    using System.Threading.Tasks;
    using Shouldly;
    using SqlStreamStore.Streams;
    using Xunit;

    public class HeadersTests : IDisposable
    {
        private const string StreamId = "a-stream";
        private readonly SqlStreamStoreHalMiddlewareFixture _fixture;

        public HeadersTests()
        {
            _fixture = new SqlStreamStoreHalMiddlewareFixture();
        }

        public void Dispose() => _fixture.Dispose();

        public static IEnumerable<object[]> Methods()
        {
            yield return new object[] { HttpMethod.Head };
            yield return new object[] { HttpMethod.Get };
        }

        [Theory, MemberData(nameof(Methods))]
        public async Task all_stream_head_link(HttpMethod method)
        {
            await _fixture.WriteNMessages(StreamId, 10);

            var position = await _fixture.StreamStore.ReadHeadPosition();

            using(var response = await _fixture.HttpClient.SendAsync(
                new HttpRequestMessage(
                    method,
                    Links.FormatBackwardLink("/stream", 20, Position.End, true))))
            {
                response.IsSuccessStatusCode.ShouldBeTrue();

                response.Headers.GetValues(Constants.Headers.HeadPosition)
                    .ShouldBe(new[] { $"{position}" });
            }
        }
    }
}