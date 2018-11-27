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

            using(var fixture = GetFixture())
            using(var streamStore = await fixture.GetStreamStore())
            {
                for(var i = 0; i < 30; i++)
                {
                    await streamStore.AppendToStream(
                        $"{streamIdPrefix}-{i}",
                        ExpectedVersion.NoStream,
                        Array.Empty<NewStreamMessage>());
                }

                var page = await streamStore.ListStreams(10);
                page.StreamIds.ShouldBe(Enumerable.Range(0, 10).Select(i => $"{streamIdPrefix}-{i}"));

                page = await page.Next();
                page.StreamIds.ShouldBe(Enumerable.Range(10, 10).Select(i => $"{streamIdPrefix}-{i}"));

                page = await page.Next();
                page.StreamIds.ShouldBe(Enumerable.Range(20, 10).Select(i => $"{streamIdPrefix}-{i}"));

                page = await page.Next();

                page.StreamIds.Length.ShouldBe(0);
            }
        }

        [Fact]
        public async Task Can_list_streams_starting_with()
        {
            const string streamIdPrefix = "stream";

            using(var fixture = GetFixture())
            using(var streamStore = await fixture.GetStreamStore())
            {
                for(var i = 0; i < 30; i++)
                {
                    await streamStore.AppendToStream(
                        $"{streamIdPrefix}-{i}",
                        ExpectedVersion.NoStream,
                        Array.Empty<NewStreamMessage>());

                    await streamStore.AppendToStream(
                        $"not-stream-{i}",
                        ExpectedVersion.NoStream,
                        Array.Empty<NewStreamMessage>());
                }

                var page = await streamStore.ListStreams(Pattern.StartsWith(streamIdPrefix), 10);
                page.StreamIds.ShouldBe(Enumerable.Range(0, 10).Select(i => $"{streamIdPrefix}-{i}"));

                page = await page.Next();
                page.StreamIds.ShouldBe(Enumerable.Range(10, 10).Select(i => $"{streamIdPrefix}-{i}"));

                page = await page.Next();
                page.StreamIds.ShouldBe(Enumerable.Range(20, 10).Select(i => $"{streamIdPrefix}-{i}"));

                page = await page.Next();

                page.StreamIds.Length.ShouldBe(0);
            }
        }

        [Fact]
        public async Task Can_list_streams_ending_with()
        {
            const string streamIdPostfix = "stream";

            using(var fixture = GetFixture())
            using(var streamStore = await fixture.GetStreamStore())
            {
                for(var i = 0; i < 30; i++)
                {
                    await streamStore.AppendToStream(
                        $"{i}-{streamIdPostfix}",
                        ExpectedVersion.NoStream,
                        Array.Empty<NewStreamMessage>());

                    await streamStore.AppendToStream(
                        $"{i}-stream-not",
                        ExpectedVersion.NoStream,
                        Array.Empty<NewStreamMessage>());
                }

                var page = await streamStore.ListStreams(Pattern.EndsWith(streamIdPostfix), 10);
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
}