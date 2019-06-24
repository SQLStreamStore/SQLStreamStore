namespace SqlStreamStore
{
    using SqlStreamStore.Streams;

    internal static class PageExtensions
    {
        public static bool TryGetETag(this ReadStreamPage page, out ETag eTag)
        {
            if(page.IsEnd)
            {
                eTag = ETag.FromStreamVersion(page.LastStreamVersion);
                return true;
            }

            if(page.ReadDirection == ReadDirection.Backward && page.FromStreamVersion == StreamVersion.End)
            {
                eTag = ETag.FromStreamVersion(page.LastStreamVersion);
                return true;
            }

            eTag = ETag.None;

            return false;
        }

        public static bool TryGetETag(this ReadAllResult page, StreamMessage[] messages, long fromPosition, out ETag eTag)
        {
            if(page.ReadDirection == ReadDirection.Backward && fromPosition == Position.End)
            {
                eTag = ETag.FromPosition(messages.Length > 0 ? messages[0].Position : Position.End);
                return true;
            }

            if(page.IsEnd && page.ReadDirection == ReadDirection.Forward)
            {
                eTag = ETag.FromPosition(messages.Length > 0 ? messages[messages.Length - 1].Position : Position.End);
                return true;
            }

            if(page.IsEnd && page.ReadDirection == ReadDirection.Backward)
            {
                eTag = ETag.FromPosition(messages.Length > 0 ? messages[0].Position : Position.End);
                return true;
            }

            eTag = ETag.None;

            return false;
        }
    }
}