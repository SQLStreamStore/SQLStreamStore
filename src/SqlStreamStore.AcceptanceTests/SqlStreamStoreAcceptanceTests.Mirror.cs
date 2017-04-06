namespace SqlStreamStore
{
    using System.Collections.Generic;
    using System.Threading.Tasks;
    using Shouldly;
    using SqlStreamStore.Infrastructure;
    using SqlStreamStore.Streams;
    using Xunit;

    public partial class StreamStoreAcceptanceTests
    {
        [Fact]
        public async Task Can_mirror_source_store_to_desintation_store()
        {
            using (var sourceFixture = GetFixture())
            {
                using (var desintationFixture = GetFixture())
                {
                    using (var sourceStore = await sourceFixture.GetStreamStore())
                    {
                        using (var destinationStore = await desintationFixture.GetStreamStore())
                        {
                            using (var mirror = new StreamStoreMirror(sourceStore, destinationStore))
                            {
                                var streamId = "stream-1";
                                var newStreamMessages = CreateNewStreamMessages(1, 2, 3);
                                var receivedMessages = new List<StreamMessage>();
                                var tcs = new TaskCompletionSource<int>();
                                using(var subscription = destinationStore.SubscribeToStream(streamId,
                                    null,
                                    (_, message) =>
                                    {
                                        receivedMessages.Add(message);
                                        if(receivedMessages.Count == newStreamMessages.Length)
                                        {
                                            tcs.SetResult(0);
                                        }
                                        return Task.CompletedTask;
                                    }))
                                {
                                    await sourceStore.AppendToStream(
                                        streamId,
                                        ExpectedVersion.NoStream,
                                        CreateNewStreamMessages(1, 2, 3));

                                    await Task.Delay(2000);

                                    var streamPage = await mirror
                                        .Desintation
                                        .ReadStreamForwards(streamId, StreamVersion.Start, 10);

                                    streamPage.Messages.Length.ShouldBe(3);
                                    for(int i = 0; i < streamPage.Messages.Length; i++)
                                    {
                                        streamPage.Messages[i].MessageId.ShouldBe(newStreamMessages[i].MessageId);
                                        streamPage.Messages[i].Type.ShouldBe(newStreamMessages[i].Type);
                                        streamPage.Messages[i].StreamId.ShouldBe(streamId);
                                        streamPage.Messages[i].JsonMetadata.ShouldBe(newStreamMessages[i].JsonMetadata);
                                        streamPage.Messages[i].StreamVersion.ShouldBe(i);
                                        (await streamPage.Messages[i].GetJsonData()).ShouldBe(newStreamMessages[i].JsonData);
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }
}
