namespace SqlStreamStore
{
    using System;
    using System.Linq;
    using System.Threading.Tasks;
    using Shouldly;
    using SqlStreamStore.Streams;
    using Xunit;

    public partial class StreamStoreAcceptanceTests
    {
        [Fact]
        public async Task Can_list_streams()
        {
            const string streamIdPrefix = "stream";

            for(var i = 0; i < 30; i++)
            {
                await store.AppendToStream(
                    $"{streamIdPrefix}-{i}",
                    ExpectedVersion.NoStream,
                    Array.Empty<NewStreamMessage>());
            }

            var page = await store.ListStreams(10);
            page.StreamIds.ShouldBe(Enumerable.Range(0, 10).Select(i => $"{streamIdPrefix}-{i}"));

            page = await page.Next();
            page.StreamIds.ShouldBe(Enumerable.Range(10, 10).Select(i => $"{streamIdPrefix}-{i}"));

            page = await page.Next();
            page.StreamIds.ShouldBe(Enumerable.Range(20, 10).Select(i => $"{streamIdPrefix}-{i}"));

            page = await page.Next();

            page.StreamIds.Length.ShouldBe(0);
        }

        [Fact]
        public async Task Can_list_streams_starting_with()
        {
            const string streamIdPrefix = "stream";

            for(var i = 0; i < 30; i++)
            {
                await store.AppendToStream(
                    $"{streamIdPrefix}-{i}",
                    ExpectedVersion.NoStream,
                    Array.Empty<NewStreamMessage>());

                await store.AppendToStream(
                    $"not-stream-{i}",
                    ExpectedVersion.NoStream,
                    Array.Empty<NewStreamMessage>());
            }

            var page = await store.ListStreams(Pattern.StartsWith(streamIdPrefix), 10);
            page.StreamIds.ShouldBe(Enumerable.Range(0, 10).Select(i => $"{streamIdPrefix}-{i}"));

            page = await page.Next();
            page.StreamIds.ShouldBe(Enumerable.Range(10, 10).Select(i => $"{streamIdPrefix}-{i}"));

            page = await page.Next();
            page.StreamIds.ShouldBe(Enumerable.Range(20, 10).Select(i => $"{streamIdPrefix}-{i}"));

            page = await page.Next();

            page.StreamIds.Length.ShouldBe(0);
        }

        [Fact]
        public async Task Can_list_streams_ending_with()
        {
            const string streamIdPostfix = "stream";

            for(var i = 0; i < 30; i++)
            {
                await store.AppendToStream(
                    $"{i}-{streamIdPostfix}",
                    ExpectedVersion.NoStream,
                    Array.Empty<NewStreamMessage>());

                await store.AppendToStream(
                    $"{i}-stream-not",
                    ExpectedVersion.NoStream,
                    Array.Empty<NewStreamMessage>());
            }

            var page = await store.ListStreams(Pattern.EndsWith(streamIdPostfix), 10);
            page.StreamIds.ShouldBe(Enumerable.Range(0, 10).Select(i => $"{i}-{streamIdPostfix}"));

            page = await page.Next();
            page.StreamIds.ShouldBe(Enumerable.Range(10, 10).Select(i => $"{i}-{streamIdPostfix}"));

            page = await page.Next();
            page.StreamIds.ShouldBe(Enumerable.Range(20, 10).Select(i => $"{i}-{streamIdPostfix}"));

            page = await page.Next();

            page.StreamIds.Length.ShouldBe(0);
        }
    }
}