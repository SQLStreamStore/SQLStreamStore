namespace SqlStreamStore.HAL.Tests
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Net;
    using System.Net.Http;
    using System.Net.Http.Headers;
    using System.Threading.Tasks;
    using Newtonsoft.Json.Linq;
    using Shouldly;
    using SqlStreamStore.Streams;
    using Xunit;

    public class StreamAppendTests : IDisposable
    {
        private const string StreamId = "a-stream";
        private readonly SqlStreamStoreHalMiddlewareFixture _fixture;

        public StreamAppendTests()
        {
            _fixture = new SqlStreamStoreHalMiddlewareFixture();
        }

        public static IEnumerable<object[]> AppendCases()
        {
            var messageId1 = Guid.NewGuid();
            var messageId2 = Guid.NewGuid();

            var jsonData = JObject.FromObject(new
            {
                property = "value"
            });

            var jsonMetadata = JObject.FromObject(new
            {
                property = "metaValue"
            });

            var bodies = new (JToken, Guid[])[]
            {
                (JObject.FromObject(new
                {
                    messageId = messageId1,
                    type = "type",
                    jsonData,
                    jsonMetadata
                }), new[] { messageId1 }),
                (JArray.FromObject(new[]
                    {
                        new
                        {
                            messageId = messageId1,
                            type = "type",
                            jsonData,
                            jsonMetadata
                        },
                        new
                        {
                            messageId = messageId2,
                            type = "type",
                            jsonData,
                            jsonMetadata
                        }
                    }),
                    new[] { messageId1, messageId2 })
            };

            var location = new Uri($"../{Constants.Streams.Stream}/{StreamId}", UriKind.Relative);

            foreach(var (body, messageIds) in bodies)
            {
                yield return new object[]
                {
                    ExpectedVersion.Any,
                    body,
                    messageIds,
                    jsonData,
                    jsonMetadata,
                    HttpStatusCode.OK,
                    null
                };
                yield return new object[]
                {
                    default(int?),
                    body,
                    messageIds,
                    jsonData,
                    jsonMetadata,
                    HttpStatusCode.OK,
                    null
                };
                yield return new object[]
                {
                    ExpectedVersion.NoStream,
                    body,
                    messageIds,
                    jsonData,
                    jsonMetadata,
                    HttpStatusCode.Created,
                    location
                };
            }
        }

        [Theory, MemberData(nameof(AppendCases))]
        public async Task expected_version(
            int? expectedVersion,
            JToken body,
            Guid[] messageIds,
            JObject jsonData,
            JObject jsonMetadata,
            HttpStatusCode statusCode,
            Uri location)
        {
            var request = new HttpRequestMessage(HttpMethod.Post, $"/{Constants.Streams.Stream}/{StreamId}")
            {
                Content = new StringContent(body.ToString())
            };

            if(expectedVersion.HasValue)
            {
                request.Headers.Add(Constants.Headers.ExpectedVersion, $"{expectedVersion}");
            }

            using(var response = await _fixture.HttpClient.SendAsync(request))
            {
                response.StatusCode.ShouldBe(statusCode);
                response.Headers.Location.ShouldBe(location);
            }

            var page = await _fixture.StreamStore.ReadStreamForwards(StreamId, 0, int.MaxValue);

            page.Status.ShouldBe(PageReadStatus.Success);
            page.Messages.Length.ShouldBe(messageIds.Length);
            for(var i = 0; i < messageIds.Length; i++)
            {
                page.Messages[i].MessageId.ShouldBe(messageIds[i]);
                page.Messages[i].Type.ShouldBe("type");
                JToken.DeepEquals(JObject.Parse(await page.Messages[i].GetJsonData()), jsonData).ShouldBeTrue();
                JToken.DeepEquals(JObject.Parse(page.Messages[i].JsonMetadata), jsonMetadata).ShouldBeTrue();
            }
        }

        [Theory]
        [InlineData(new[] { ExpectedVersion.NoStream, ExpectedVersion.NoStream })]
        [InlineData(new[] { ExpectedVersion.NoStream, 2 })]
        public async Task wrong_expected_version(int[] expectedVersions)
        {
            var jsonData = JObject.FromObject(new
            {
                property = "value"
            });

            var jsonMetadata = JObject.FromObject(new
            {
                property = "metaValue"
            });

            for(var i = 0; i < expectedVersions.Length - 1; i++)
            {
                using(await _fixture.HttpClient.SendAsync(
                    new HttpRequestMessage(HttpMethod.Post, $"/{Constants.Streams.Stream}/{StreamId}")
                    {
                        Headers =
                        {
                            { Constants.Headers.ExpectedVersion, $"{expectedVersions[i]}" }
                        },
                        Content = new StringContent(JObject.FromObject(new
                        {
                            messageId = Guid.NewGuid(),
                            type = "type",
                            jsonData,
                            jsonMetadata
                        }).ToString())
                        {
                            Headers =
                            {
                                ContentType = new MediaTypeHeaderValue("application/json")
                            }
                        }
                    }))
                { }
            }

            using(var response = await _fixture.HttpClient.SendAsync(
                new HttpRequestMessage(HttpMethod.Post, $"/{Constants.Streams.Stream}/{StreamId}")
                {
                    Headers =
                    {
                        { Constants.Headers.ExpectedVersion, $"{expectedVersions[expectedVersions.Length - 1]}" }
                    },
                    Content = new StringContent(JObject.FromObject(new
                    {
                        messageId = Guid.NewGuid(),
                        type = "type",
                        jsonData,
                        jsonMetadata
                    }).ToString())
                    {
                        Headers =
                        {
                            ContentType = new MediaTypeHeaderValue("application/json")
                        }
                    }
                }))
            {
                response.StatusCode.ShouldBe(HttpStatusCode.Conflict);
                response.Content.Headers.ContentType.ShouldBe(new MediaTypeHeaderValue(
                    Constants.MediaTypes.HalJson));
            }

            var page = await _fixture.StreamStore.ReadStreamForwards(StreamId, 0, int.MaxValue);

            page.Status.ShouldBe(PageReadStatus.Success);
            page.Messages.Length.ShouldBe(expectedVersions.Length - 1);
        }

        private static IEnumerable<string> MalformedRequests()
        {
            var messageId = Guid.NewGuid();

            const string type = "type";

            var jsonData = JObject.FromObject(new
            {
                property = "value"
            });

            var jsonMetadata = JObject.FromObject(new
            {
                property = "metaValue"
            });

            yield return string.Empty;
            yield return "{}";

            yield return JObject.FromObject(new
            {
                messageId = Guid.Empty,
                type,
                jsonData,
                jsonMetadata
            }).ToString();

            yield return JObject.FromObject(new
            {
                type,
                jsonData,
                jsonMetadata
            }).ToString();

            yield return JObject.FromObject(new
            {
                messageId,
                jsonData,
                jsonMetadata
            }).ToString();

            yield return $@"{{ ""messageId"": ""{messageId}"", ""type"": ""{type}"", ""jsonData"": {{ }}";
            yield return $@"{{ ""messageId"": ""{messageId}"", ""type"": ""{type}"", ""jsonMetadata"": {{ }}";
        }

        public static IEnumerable<object[]> MalformedRequestCases()
            => MalformedRequests().Select(s => new object[] { s });

        [Theory, MemberData(nameof(MalformedRequestCases))]
        public async Task malformed_request_body(string malformedRequest)
        {
            using(var response = await _fixture.HttpClient.SendAsync(
                new HttpRequestMessage(HttpMethod.Post, $"/{Constants.Streams.Stream}/{StreamId}")
                {
                    Content = new StringContent(malformedRequest)
                    {
                        Headers = { ContentType = new MediaTypeHeaderValue(Constants.MediaTypes.HalJson) }
                    }
                }))
            {
                response.StatusCode.ShouldBe(HttpStatusCode.BadRequest);
            }
        }

        public static IEnumerable<object[]> BadExpectedVersionCases()
        {
            var random = new Random();

            var maximumBadExpectedVersion = Constants.Headers.MinimumExpectedVersion - 1;

            yield return new object[] { maximumBadExpectedVersion };
            for(var i = 0; i < 5; i++)
            {
                yield return new object[] { random.Next(int.MinValue, maximumBadExpectedVersion) };
            }
        }

        [Theory, MemberData(nameof(BadExpectedVersionCases))]
        public async Task bad_expected_version(int badExpectedVersion)
        {
            using(var response = await _fixture.HttpClient.SendAsync(
                new HttpRequestMessage(HttpMethod.Post, $"/{Constants.Streams.Stream}/{StreamId}")
                {
                    Headers =
                    {
                        {Constants.Headers.ExpectedVersion, $"{badExpectedVersion}"}
                    },
                    Content = new StringContent(JObject.FromObject(new
                    {
                        messageId = Guid.NewGuid(),
                        jsonData = new { },
                        type = "type"
                    }).ToString())
                    {
                        Headers =
                        {
                            ContentType = new MediaTypeHeaderValue(Constants.MediaTypes.HalJson)
                        }
                    }
                }))
            {
                response.StatusCode.ShouldBe(HttpStatusCode.BadRequest);
            }
        }

        public void Dispose() => _fixture.Dispose();
    }
}