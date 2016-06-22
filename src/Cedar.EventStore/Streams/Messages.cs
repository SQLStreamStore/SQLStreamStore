namespace Cedar.EventStore.Streams
{
    public static class Messages
    {
        public static string AppendFailedWrongExpectedVersion(string streamId, int expectedVersion) 
            => $"Append failed due to WrongExpectedVersion.Stream: {streamId}, Expected version: {expectedVersion}";

        public static string DeleteStreamFailedWrongExpectedVersion(string streamId, int expectedVersion)
            => $"Delete stream failed due to WrongExpectedVersion.Stream: {streamId}, Expected version: {expectedVersion}.";
    }
}