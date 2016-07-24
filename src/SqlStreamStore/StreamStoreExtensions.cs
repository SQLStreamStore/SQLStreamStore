namespace SqlStreamStore
{
    using System.Collections.Generic;
    using System.Linq;
    using System.Threading.Tasks;
    using SqlStreamStore.Streams;

    public static class StreamStoreExtensions
    {
        public static Task AppendToStream(
            this IStreamStore streamStore,
            string streamId,
            int expectedVersion,
            NewStreamMessage newStreamMessage)
        {
            return streamStore.AppendToStream(streamId, expectedVersion, new[] { newStreamMessage });
        }

        public static Task AppendToStream(
            this IStreamStore streamStore,
            string streamId,
            int expectedVersion,
            IEnumerable<NewStreamMessage> events)
        {
            return streamStore.AppendToStream(streamId, expectedVersion, events.ToArray());
        }
    }
}