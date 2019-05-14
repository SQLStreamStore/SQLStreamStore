namespace SqlStreamStore.HAL.Tests
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Net;
    using System.Threading.Tasks;
    using Shouldly;
    using SqlStreamStore.Streams;
    using Xunit;
    using Xunit.Abstractions;

    public class CanonicalUrlTests
    {
        delegate string FormatAllStreamLink(
            long fromPosition,
            int maxCount,
            bool prefetchJsonData);

        delegate string FormatStreamLink(
            StreamId streamId,
            int fromVersionInclusive,
            int maxCount,
            bool prefetchJsonData);
        
        private const string StreamId = "a-stream";
        private readonly SqlStreamStoreHalMiddlewareFixture _fixture;

        public CanonicalUrlTests(ITestOutputHelper output)
        {
            _fixture = new SqlStreamStoreHalMiddlewareFixture(output);
        }

        private static IEnumerable<string> GetQueryStrings(bool forward, bool prefetch)
        {
            IEnumerable<string[]> GetPermutations(string[] items, int length)
                => length == 1
                    ? items.Select(item => new[] { item })
                    : GetPermutations(items, length - 1)
                        .SelectMany(permutation => items.Where(item => !permutation.Contains(item)),
                            (t1, t2) => t1.Concat(new[] { t2 }).ToArray())
            ;

            var d = $"d={(forward ? 'f' : 'b')}";
            var m = "m=20";
            var e = $"e={(prefetch ? '1' : '0')}";
            var p = "p=0";

            var parameters = new[]
            {
                d, m, e, p
            };

            return
                from set in GetPermutations(parameters, 4)
                select string.Join('&', set.Where(x => x != null));
        }
        
        private static IEnumerable<(string, Uri)> NonCanonicalAll()
        {
            var bools = new[] { true, false };
         
            var formatters = new Dictionary<bool, FormatAllStreamLink>
            {
                [true] = LinkFormatter.ReadAllForwards,
                [false] = LinkFormatter.ReadAllBackwards
            };

            return
                from prefetch in bools
                from forward in bools
                let format = formatters[forward]
                let canonicalUri = new Uri(
                    format(Position.Start, 20, prefetch),
                    UriKind.Relative)
                from queryString in GetQueryStrings(forward, prefetch)
                    // query strings are supposed to be case sensitive!!
                    //.Concat(new[] { $"d={(forward ? 'F' : 'B')}&M=20&P=0{(prefetch ? "&E" : string.Empty)}" })
                where !canonicalUri.OriginalString.EndsWith($"?{queryString}")
                select ($"{Constants.Paths.AllStream}?{queryString}", canonicalUri);
        }
        private static IEnumerable<(string, Uri)> NonCanonicalStream()
        {
            var bools = new[] { true, false };
         
            var formatters = new Dictionary<bool, FormatStreamLink>
            {
                [true] = LinkFormatter.ReadStreamForwards,
                [false] = LinkFormatter.ReadStreamBackwards
            };

            return
                from prefetch in bools
                from forward in bools
                let format = formatters[forward]
                let canonicalUri = new Uri(
                    $"../{format(StreamId, StreamVersion.Start, 20, prefetch)}",
                    UriKind.Relative)
                from queryString in GetQueryStrings(forward, prefetch)
                // query strings are supposed to be case sensitive!!
                //.Concat(new[] { $"d={(forward ? 'F' : 'B')}&M=20&P=0{(prefetch ? "&E" : string.Empty)}" })
                where !canonicalUri.OriginalString.EndsWith($"?{queryString}")
                select ($"{LinkFormatter.Stream(StreamId)}?{queryString}", canonicalUri);
        }
        
        public static IEnumerable<object[]> NonCanonicalUriCases()
            => NonCanonicalAll().Concat(NonCanonicalStream())
                .Select(x => new object[] { x.Item1, x.Item2 })
                .ToArray();

        [Theory, MemberData(nameof(NonCanonicalUriCases))]
        public async Task non_canonical_uri(string requestUri, Uri expectedLocation)
        {
            using(var response = await _fixture.HttpClient.GetAsync(requestUri))
            {
                response.StatusCode.ShouldBe(HttpStatusCode.PermanentRedirect);
                response.Headers.Location.ShouldBe(expectedLocation);
            }
        }
    }
}