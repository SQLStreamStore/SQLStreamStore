﻿namespace SqlStreamStore
{
    using System.Threading.Tasks;
    using Shouldly;
    using Xunit;

    public abstract partial class StreamStoreAcceptanceTests
    {
        [Fact]
        public async Task Given_empty_store_when_get_head_position_Then_should_be_minus_one()
        {
            using(var fixture = GetFixture())
            {
                var store = await fixture.GetStreamStore();

                var head = await store.ReadHeadPosition();

                head.ShouldBe(-1);
            }
        }

        [Fact]
        public async Task Given_store_with_messages_then_can_get_head_position()
        {
            using (var fixture = GetFixture())
            {
                var store = await fixture.GetStreamStore();

                await store.AppendToStream("stream-1", ExpectedVersion.NoStream, CreateNewStreamMessages(1, 2, 3));

                var head = await store.ReadHeadPosition();

                head.ShouldBe(2);
            }
        }
    }
}
