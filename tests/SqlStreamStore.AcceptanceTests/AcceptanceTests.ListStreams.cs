namespace SqlStreamStore
{
    using System;
    using System.Linq;
    using System.Threading.Tasks;
    using Shouldly;
    using SqlStreamStore.Streams;
    using Xunit;

    public partial class AcceptanceTests
    {
        [Theory]
        [InlineData("stream")]
        [InlineData("stream/1")]
        [InlineData("stream%1")]
        public async Task Can_list_streams(string streamIdPrefix)
        {

            for(var i = 0; i < 30; i++)
            {
                await Store.AppendToStream(
                    $"{streamIdPrefix}-{i}",
                    ExpectedVersion.NoStream,
                    Array.Empty<NewStreamMessage>());
            }

            var page = await Store.ListStreams(10);
            page.StreamIds.ShouldBe(Enumerable.Range(0, 10).Select(i => $"{streamIdPrefix}-{i}"));

            page = await page.Next();
            page.StreamIds.ShouldBe(Enumerable.Range(10, 10).Select(i => $"{streamIdPrefix}-{i}"));

            page = await page.Next();
            page.StreamIds.ShouldBe(Enumerable.Range(20, 10).Select(i => $"{streamIdPrefix}-{i}"));

            page = await page.Next();

            page.StreamIds.Length.ShouldBe(0);
        }

        [Theory]
        [InlineData("stream")]
        [InlineData("stream/1")]
        [InlineData("stream%1")]
        public async Task Can_list_streams_starting_with(string streamIdPrefix)
        {
            for(var i = 0; i < 30; i++)
            {
                await Store.AppendToStream(
                    $"{streamIdPrefix}-{i}",
                    ExpectedVersion.NoStream,
                    Array.Empty<NewStreamMessage>());

                await Store.AppendToStream(
                    $"not-stream-{i}",
                    ExpectedVersion.NoStream,
                    Array.Empty<NewStreamMessage>());
            }

            var page = await Store.ListStreams(Pattern.StartsWith(streamIdPrefix), 10);
            page.StreamIds.ShouldBe(Enumerable.Range(0, 10).Select(i => $"{streamIdPrefix}-{i}"));

            page = await page.Next();
            page.StreamIds.ShouldBe(Enumerable.Range(10, 10).Select(i => $"{streamIdPrefix}-{i}"));

            page = await page.Next();
            page.StreamIds.ShouldBe(Enumerable.Range(20, 10).Select(i => $"{streamIdPrefix}-{i}"));

            page = await page.Next();

            page.StreamIds.Length.ShouldBe(0);
        }

        [Theory]
        [InlineData("stream")]
        [InlineData("stream/1")]
        [InlineData("stream%1")]
        public async Task Can_list_streams_ending_with(string streamIdPostfix)
        {
            for(var i = 0; i < 30; i++)
            {
                await Store.AppendToStream(
                    $"{i}-{streamIdPostfix}",
                    ExpectedVersion.NoStream,
                    Array.Empty<NewStreamMessage>());

                await Store.AppendToStream(
                    $"{i}-stream-not",
                    ExpectedVersion.NoStream,
                    Array.Empty<NewStreamMessage>());
            }

            var page = await Store.ListStreams(Pattern.EndsWith(streamIdPostfix), 10);
            page.StreamIds.ShouldBe(Enumerable.Range(0, 10).Select(i => $"{i}-{streamIdPostfix}"));

            page = await page.Next();
            page.StreamIds.ShouldBe(Enumerable.Range(10, 10).Select(i => $"{i}-{streamIdPostfix}"));

            page = await page.Next();
            page.StreamIds.ShouldBe(Enumerable.Range(20, 10).Select(i => $"{i}-{streamIdPostfix}"));

            page = await page.Next();

            page.StreamIds.Length.ShouldBe(0);
        }

        [Fact]
        public async Task When_list_streams_after_deletion_empty_results_should_not_be_returned()
        {
            await Store.AppendToStream("stream-1", ExpectedVersion.Any, CreateNewStreamMessages(1));
            await Store.AppendToStream("stream-2", ExpectedVersion.Any, CreateNewStreamMessages(2));

            await Store.DeleteStream("stream-1");

            var page = await Store.ListStreams(Pattern.Anything());

            page.StreamIds.ShouldNotContain((string) default);
            page.StreamIds.ShouldNotContain("stream-1");
        }
    }
}