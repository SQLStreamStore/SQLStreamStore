namespace SqlStreamStore.V1
{
    using System;
    using System.Collections.Generic;
    using System.Collections.Immutable;
    using System.Linq;
    using System.Net;
    using System.Net.Http;
    using System.Net.Http.Headers;
    using System.Reflection;
    using System.Threading.Tasks;
    using Shouldly;
    using Xunit;
    using Xunit.Abstractions;

    public class ClientErrorTests : IDisposable
    {
        private static readonly ImmutableHashSet<HttpMethod> s_AllMethods
            = ImmutableHashSet.Create(
                (from property in typeof(HttpMethod).GetProperties(BindingFlags.Public | BindingFlags.Static)
                    where property.PropertyType == typeof(HttpMethod)
                    select (HttpMethod) property.GetValue(null)).ToArray());

        private static readonly MediaTypeWithQualityHeaderValue s_HalJson =
            MediaTypeWithQualityHeaderValue.Parse(Constants.MediaTypes.HalJson);

        private static readonly MediaTypeWithQualityHeaderValue s_TextMarkdown =
            MediaTypeWithQualityHeaderValue.Parse(Constants.MediaTypes.TextMarkdown);

        private static readonly MediaTypeWithQualityHeaderValue s_TextPlain =
            MediaTypeWithQualityHeaderValue.Parse("text/plain");

        private static readonly (string requestUri, MediaTypeWithQualityHeaderValue[] notAcceptable, HttpMethod[]
            methods)
            [] s_ResourceConfigurations =
            {
                ("/", new[] { s_TextMarkdown, s_TextPlain }, new[]
                {
                    HttpMethod.Get,
                    HttpMethod.Head
                }),
                ("/stream", new[] { s_TextMarkdown, s_TextPlain }, new[]
                {
                    HttpMethod.Get,
                    HttpMethod.Head
                }),
                ($"/{Constants.Paths.Streams}/a-stream", new[] { s_TextMarkdown, s_TextPlain }, new[]
                {
                    HttpMethod.Get,
                    HttpMethod.Head,
                    HttpMethod.Post,
                    HttpMethod.Delete
                }),
                ($"/{Constants.Paths.Streams}/a-stream/0", new[] { s_TextMarkdown, s_TextPlain }, new[]
                {
                    HttpMethod.Get,
                    HttpMethod.Head,
                    HttpMethod.Delete
                }),
                ($"/{Constants.Paths.Streams}/a-stream/metadata", new[] { s_TextMarkdown, s_TextPlain }, new[]
                {
                    HttpMethod.Get,
                    HttpMethod.Head,
                    HttpMethod.Post
                }),
                ("/docs/doc", new[] { s_HalJson, s_TextPlain }, new[]
                {
                    HttpMethod.Get,
                    HttpMethod.Head
                })
            };


        private readonly SqlStreamStoreHalMiddlewareFixture _fixture;

        public ClientErrorTests(ITestOutputHelper output)
        {
            _fixture = new SqlStreamStoreHalMiddlewareFixture(output);
        }

        public static IEnumerable<object[]> MethodNotAllowedCases()
            => from _ in s_ResourceConfigurations
                let allowed = _.methods.Concat(new[] { HttpMethod.Options }).ToArray() // options are always allowed
                from method in s_AllMethods.Except(allowed)
                select new object[] { _.requestUri, method, allowed };

        [Theory, MemberData(nameof(MethodNotAllowedCases))]
        public async Task method_not_allowed(string requestUri, HttpMethod method, HttpMethod[] allowed)
        {
            using(var response = await _fixture.HttpClient.SendAsync(new HttpRequestMessage(method, requestUri)))
            {
                response.StatusCode.ShouldBe(HttpStatusCode.MethodNotAllowed);

                response.Headers.TryGetValues(Constants.Headers.Allowed, out var allowedHeaders).ShouldBeTrue();

                allowedHeaders.ShouldBe(allowed.Select(x => x.Method), true);
            }
        }

        public static IEnumerable<object[]> NotAcceptableCases()
            => from _ in s_ResourceConfigurations
                from method in _.methods.Except(new[] { HttpMethod.Delete }).ToArray() // deletes return 204
                from notAcceptable in _.notAcceptable
                select new object[] { _.requestUri, method, notAcceptable };

        [Theory, MemberData(nameof(NotAcceptableCases))]
        public async Task not_acceptable(
            string requestUri,
            HttpMethod method,
            MediaTypeWithQualityHeaderValue mediaType)
        {
            using(var response = await _fixture.HttpClient.SendAsync(new HttpRequestMessage(method, requestUri)
            {
                Headers = { Accept = { mediaType } }
            }))
            {
                response.StatusCode.ShouldBe(HttpStatusCode.NotAcceptable);
            }
        }

        public void Dispose() => _fixture.Dispose();
    }
}