namespace SqlStreamStore.V1
{
    using System;
    using System.Collections.Generic;
    using System.IO;
    using System.Linq;
    using System.Net;
    using System.Net.Http;
    using System.Net.Http.Headers;
    using System.Threading.Tasks;
    using Shouldly;
    using SqlStreamStore.V1.StreamMessages;
    using SqlStreamStore.V1.StreamMetadata;
    using SqlStreamStore.V1.Streams;
    using Xunit;
    using Xunit.Abstractions;

    public class DocumentationTests : IDisposable
    {
        private static readonly MediaTypeWithQualityHeaderValue[] s_MarkdownMediaTypes =
        {
            new MediaTypeWithQualityHeaderValue(Constants.MediaTypes.TextMarkdown), 
            new MediaTypeWithQualityHeaderValue(Constants.MediaTypes.TextMarkdown)
            {
                CharSet = "utf-8"
            },
            new MediaTypeWithQualityHeaderValue(Constants.MediaTypes.Any) 
        };
        private static readonly IDictionary<string, Type> s_documentedRels = new Dictionary<string, Type>
        {
            ["append"] = typeof(StreamResource),
            ["delete-stream"] = typeof(StreamResource),
            ["delete-message"] = typeof(StreamMessageResource),
            ["metadata"] = typeof(StreamMetadataResource)
        };

        private readonly SqlStreamStoreHalMiddlewareFixture _fixture;

        public DocumentationTests(ITestOutputHelper output)
        {
            _fixture = new SqlStreamStoreHalMiddlewareFixture(output);
        }

        public static IEnumerable<object[]> DocumentationCases()
            => from pair in s_documentedRels
                from mediaType in s_MarkdownMediaTypes
                select new object[]
                {
                    pair.Key,
                    mediaType,
                    pair.Value.Assembly.GetManifestResourceStream(pair.Value, $"Schema.{pair.Key}.schema.md")
                };

        [Fact]
        public async Task documentation_not_found()
        {
            using(var response = await _fixture.HttpClient.SendAsync(new HttpRequestMessage(HttpMethod.Get, $"/docs/{Guid.NewGuid()}")
            {
                Headers = { Accept = { MediaTypeWithQualityHeaderValue.Parse(Constants.MediaTypes.TextMarkdown) } }
            }))
            {
                response.StatusCode.ShouldBe(HttpStatusCode.NotFound);
            }
        }

        [Theory, MemberData(nameof(DocumentationCases))]
        public async Task documentation(string rel, MediaTypeWithQualityHeaderValue accept, Stream content)
        {
            content.ShouldNotBeNull();
            
            using (var expectedDocument = new MemoryStream())
            using (var actualDocument = new MemoryStream())
            using(var response = await _fixture.HttpClient.SendAsync(new HttpRequestMessage(HttpMethod.Get, $"/docs/{rel}")
            {
                Headers = { Accept = { accept } }
            }))
            {
                response.StatusCode.ShouldBe(HttpStatusCode.OK);

                await content.CopyToAsync(expectedDocument);

                await response.Content.CopyToAsync(actualDocument);
                
                expectedDocument.ToArray().ShouldBe(actualDocument.ToArray());
            }
        }

        public void Dispose() => _fixture.Dispose();
    }
}