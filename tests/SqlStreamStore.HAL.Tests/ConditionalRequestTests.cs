namespace SqlStreamStore.HAL.Tests
{
    using System;
    using System.Collections.Generic;
    using System.Net;
    using System.Net.Http;
    using System.Threading.Tasks;
    using Shouldly;
    using SqlStreamStore.Streams;
    using Xunit;
    using Xunit.Abstractions;

    public class ConditionalRequestTests : IDisposable
    {
        private readonly SqlStreamStoreHalMiddlewareFixture _fixture;

        public ConditionalRequestTests(ITestOutputHelper output)
        {
            _fixture = new SqlStreamStoreHalMiddlewareFixture(output, true);
        }

        public static IEnumerable<object[]> IfNoneMatchCases()
        {
            var message = new NewStreamMessage(Guid.NewGuid(), "type", "{}", "{}");
            const string streamId = "stream-1";
            yield return new object[]
            {
                "/stream",
                new Func<IStreamStore, Task>(
                    streamStore => streamStore.AppendToStream(streamId, ExpectedVersion.NoStream, message)),
            };
            yield return new object[]
            {
                $"/{Constants.Paths.Streams}/{streamId}",
                new Func<IStreamStore, Task>(
                    streamStore => streamStore.AppendToStream(streamId, ExpectedVersion.NoStream, message))
            };
            yield return new object[]
            {
                $"/{Constants.Paths.Streams}/{streamId}/metadata",
                new Func<IStreamStore, Task>(
                    streamStore => streamStore.SetStreamMetadata(streamId, ExpectedVersion.NoStream, 1))
            };
        }

        [Theory, MemberData(nameof(IfNoneMatchCases))]
        public async Task when_match(string path, Func<IStreamStore, Task> operation)
        {
            await operation(_fixture.StreamStore);

            using(var unconditionalResponse = await _fixture.HttpClient.SendAsync(
                new HttpRequestMessage(HttpMethod.Get, path)))
            {
                unconditionalResponse.Headers.ETag.ShouldNotBeNull();

                using(var conditionalResponse = await _fixture.HttpClient.SendAsync(
                    new HttpRequestMessage(HttpMethod.Get, path)
                    {
                        Headers = { IfNoneMatch = { unconditionalResponse.Headers.ETag } }
                    }))
                {
                    conditionalResponse.StatusCode.ShouldBe(HttpStatusCode.NotModified);
                }
            }
        }

        public void Dispose() => _fixture.Dispose();
    }
}