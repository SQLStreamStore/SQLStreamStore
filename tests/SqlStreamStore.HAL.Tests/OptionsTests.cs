namespace SqlStreamStore.HAL.Tests
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Net;
    using System.Net.Http;
    using System.Threading.Tasks;
    using Shouldly;
    using Xunit;
    using Xunit.Abstractions;

    public class OptionsTests : IDisposable
    {
        private readonly SqlStreamStoreHalMiddlewareFixture _fixture;

        public OptionsTests(ITestOutputHelper output)
        {
            _fixture = new SqlStreamStoreHalMiddlewareFixture(output);
        }

        public void Dispose() => _fixture.Dispose();

        public static IEnumerable<object[]> OptionsAllowedMethodCases()
        {
            yield return new object[]
            {
                "/stream",
                new[] { HttpMethod.Get, HttpMethod.Head, HttpMethod.Options }
            };

            yield return new object[]
            {
                "/stream/123",
                new[] { HttpMethod.Get, HttpMethod.Head, HttpMethod.Options }
            };

            yield return new object[]
            {
                $"/{Constants.Streams.Stream}/a-stream",
                new[] { HttpMethod.Get, HttpMethod.Head, HttpMethod.Options, HttpMethod.Delete, HttpMethod.Post }
            };

            yield return new object[]
            {
                $"/{Constants.Streams.Stream}/a-stream/0",
                new[] { HttpMethod.Get, HttpMethod.Head, HttpMethod.Delete, HttpMethod.Options }
            };

            yield return new object[]
            {
                $"/{Constants.Streams.Stream}/a-stream/{Guid.Empty}",
                new[] { HttpMethod.Get, HttpMethod.Head, HttpMethod.Delete, HttpMethod.Options }
            };

            yield return new object[]
            {
                $"/{Constants.Streams.Stream}/a-stream/metadata",
                new[] { HttpMethod.Get, HttpMethod.Head, HttpMethod.Post, HttpMethod.Options }
            };

            yield return new object[]
            {
                "/docs/doc",
                new[] { HttpMethod.Get, HttpMethod.Head, HttpMethod.Options }
            };
        }

        [Theory, MemberData(nameof(OptionsAllowedMethodCases))]
        public async Task options_returns_the_correct_cors_headers(string requestUri, HttpMethod[] allowedMethods)
        {
            using(var response = await _fixture.HttpClient.SendAsync(
                new HttpRequestMessage(HttpMethod.Options, requestUri)))
            {
                response.StatusCode.ShouldBe(HttpStatusCode.OK);
                response.Headers.GetValues(Constants.Headers.AccessControl.AllowHeaders)
                    .SelectMany(x => x.Split(','))
                    .ShouldBe(new[]
                        {
                            Constants.Headers.ContentType,
                            Constants.Headers.XRequestedWith,
                            Constants.Headers.Authorization
                        },
                        true);
                response.Headers.GetValues(Constants.Headers.AccessControl.AllowOrigin)
                    .SelectMany(x => x.Split(','))
                    .ShouldBe(new[] { "*" }, true);
                response.Headers.GetValues(Constants.Headers.AccessControl.AllowMethods)
                    .SelectMany(x => x.Split(','))
                    .ShouldBe(allowedMethods.Select(_ => _.Method), true);
            }
        }
    }
}