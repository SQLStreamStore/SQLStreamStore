namespace SqlStreamStore.HAL.Tests
{
    using System;
    using System.Collections.Generic;
    using System.Net;
    using System.Net.Http.Headers;
    using System.Threading.Tasks;
    using Shouldly;
    using SqlStreamStore.Streams;
    using Xunit;

    public class StreamNavigationTests : IDisposable
    {
        private const string FirstLinkQuery = "d=f&m=20&p=0&e=0";
        private const string LastLinkQuery = "d=b&m=20&p=-1&e=0";
        private const string StreamId = "a-stream";

        private readonly SqlStreamStoreHalMiddlewareFixture _fixture;

        public StreamNavigationTests()
        {
            _fixture = new SqlStreamStoreHalMiddlewareFixture(true);
        }

        public void Dispose() => _fixture.Dispose();

        public static IEnumerable<object[]> GetNoMessagesPagingCases()
        {
            yield return new object[] { "stream", string.Empty, HttpStatusCode.OK };
            yield return new object[] { $"streams/{StreamId}", "../", HttpStatusCode.NotFound };
        }

        private static bool IsAllStream(string path) => path == "stream";

        [Theory, MemberData(nameof(GetNoMessagesPagingCases))]
        public async Task read_head_link_no_messages(string path, string root, HttpStatusCode statusCode)
        {
            using(var response = await _fixture.HttpClient.GetAsync(path))
            {
                response.StatusCode.ShouldBe(statusCode);
                var eTag = IsAllStream(path)
                    ? ETag.FromPosition(Position.End)
                    : ETag.FromStreamVersion(StreamVersion.End);
                response.Headers.ETag.ShouldBe(new EntityTagHeaderValue(eTag));

                var resource = await response.AsHal();

                var links = Links
                    .FromRequestMessage(response.RequestMessage)
                    .Index()
                    .Find()
                    .Add(Constants.Relations.Self, $"{path}?{LastLinkQuery}", !IsAllStream(path) ? StreamId : null)
                    .Add(Constants.Relations.Last, $"{path}?{LastLinkQuery}")
                    .Add(Constants.Relations.First, $"{path}?{FirstLinkQuery}")
                    .Add(Constants.Relations.Feed, $"{path}?{LastLinkQuery}", !IsAllStream(path) ? StreamId : null);

                if(!IsAllStream(path))
                {
                    links.Add(Constants.Relations.Metadata, $"{path}/metadata");
                }

                resource.ShouldLink(links);
            }
        }

        public static IEnumerable<object[]> GetPagingCases()
        {
            yield return new object[] { "stream", string.Empty };
            yield return new object[] { $"streams/{StreamId}", "../" };
        }

        [Theory, MemberData(nameof(GetPagingCases))]
        public async Task read_head_link_when_multiple_pages(string path, string root)
        {
            var result = await _fixture.WriteNMessages(StreamId, 30);

            using(var response = await _fixture.HttpClient.GetAsync($"{path}"))
            {
                response.StatusCode.ShouldBe(HttpStatusCode.OK);
                var eTag = IsAllStream(path)
                    ? ETag.FromPosition(result.CurrentPosition)
                    : ETag.FromStreamVersion(result.CurrentVersion);
                response.Headers.ETag.ShouldBe(new EntityTagHeaderValue(eTag));

                var resource = await response.AsHal();

                var links = Links
                    .FromRequestMessage(response.RequestMessage)
                    .Index()
                    .Find()
                    .Add(Constants.Relations.Self, $"{path}?{LastLinkQuery}", !IsAllStream(path) ? StreamId : null)
                    .Add(Constants.Relations.Last, $"{path}?{LastLinkQuery}")
                    .Add(Constants.Relations.Previous, $"{path}?d=b&m=20&p=9&e=0")
                    .Add(Constants.Relations.First, $"{path}?{FirstLinkQuery}")
                    .Add(Constants.Relations.Feed, $"{path}?{LastLinkQuery}", !IsAllStream(path) ? StreamId : null);

                if(!IsAllStream($"{path}"))
                {
                    links.Add(Constants.Relations.Metadata, $"{path}/metadata");
                }

                resource.ShouldLink(links);
            }
        }

        [Theory, MemberData(nameof(GetPagingCases))]
        public async Task read_first_link(string path, string root)
        {
            var result = await _fixture.WriteNMessages(StreamId, 10);

            using(var response = await _fixture.HttpClient.GetAsync($"{path}?{FirstLinkQuery}"))
            {
                response.StatusCode.ShouldBe(HttpStatusCode.OK);
                var eTag = IsAllStream(path)
                    ? ETag.FromPosition(result.CurrentPosition)
                    : ETag.FromStreamVersion(result.CurrentVersion);
                response.Headers.ETag.ShouldBe(new EntityTagHeaderValue(eTag));

                var resource = await response.AsHal();

                var links = Links
                    .FromRequestMessage(response.RequestMessage)
                    .Index()
                    .Find()
                    .Add(Constants.Relations.Self, $"{path}?{FirstLinkQuery}", !IsAllStream(path) ? StreamId : null)
                    .Add(Constants.Relations.Last, $"{path}?{LastLinkQuery}")
                    .Add(Constants.Relations.First, $"{path}?{FirstLinkQuery}")
                    .Add(Constants.Relations.Feed, $"{path}?{FirstLinkQuery}", !IsAllStream(path) ? StreamId : null);

                if(!IsAllStream(path))
                {
                    links.Add(Constants.Relations.Metadata, $"{path}/metadata");
                }

                resource.ShouldLink(links);
            }
        }

        [Theory, MemberData(nameof(GetPagingCases))]
        public async Task read_first_link_when_multiple_pages(string path, string root)
        {
            await _fixture.WriteNMessages(StreamId, 30);

            using(var response = await _fixture.HttpClient.GetAsync($"{path}?{FirstLinkQuery}"))
            {
                response.StatusCode.ShouldBe(HttpStatusCode.OK);

                var resource = await response.AsHal();

                var links = Links
                    .FromRequestMessage(response.RequestMessage)
                    .Index()
                    .Find()
                    .Add(Constants.Relations.Self, $"{path}?{FirstLinkQuery}", !IsAllStream(path) ? StreamId : null)
                    .Add(Constants.Relations.Last, $"{path}?{LastLinkQuery}")
                    .Add(Constants.Relations.Next, $"{path}?d=f&m=20&p=20&e=0")
                    .Add(Constants.Relations.First, $"{path}?{FirstLinkQuery}")
                    .Add(Constants.Relations.Feed, $"{path}?{FirstLinkQuery}", !IsAllStream(path) ? StreamId : null);

                if(!IsAllStream($"{path}"))
                {
                    links.Add(Constants.Relations.Metadata, $"{path}/metadata");
                }

                resource.ShouldLink(links);
            }
        }

        [Fact]
        public async Task read_stream_should_include_the_last_position_and_version()
        {
            await _fixture.WriteNMessages(StreamId, 30);

            var page = await _fixture.StreamStore.ReadStreamForwards(StreamId, StreamVersion.Start, 10, false);

            using(var response = await _fixture.HttpClient.GetAsync($"/{Constants.Streams.Stream}/a-stream"))
            {
                response.StatusCode.ShouldBe(HttpStatusCode.OK);

                var resource = await response.AsHal();

                ((int) resource.State.lastStreamVersion).ShouldBe(page.LastStreamVersion);
                ((long) resource.State.lastStreamPosition).ShouldBe(page.LastStreamPosition);
            }
        }
    }
}