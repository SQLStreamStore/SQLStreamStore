namespace SqlStreamStore.HAL
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

        public static bool TryGetETag(this ReadAllPage page, long fromPosition, out ETag eTag)
        {
            if(page.Direction == ReadDirection.Backward && fromPosition == Position.End)
            {
                eTag = ETag.FromPosition(page.Messages.Length > 0 ? page.Messages[0].Position : Position.End);
                return true;
            }

            if(page.IsEnd && page.Direction == ReadDirection.Forward)
            {
                eTag = ETag.FromPosition(page.Messages.Length > 0 ? page.Messages[page.Messages.Length - 1].Position : Position.End);
                return true;
            }

            if(page.IsEnd && page.Direction == ReadDirection.Backward)
            {
                eTag = ETag.FromPosition(page.Messages.Length > 0 ? page.Messages[0].Position : Position.End);
                return true;
            }

            eTag = ETag.None;

            return false;
        }
    }
}