namespace SqlStreamStore
{
    internal static class MySqlErrorMessages
    {
        public static string AppendFailedDeadlock(string streamId, int expectedVersion, int times) 
            => $"Append failed due to deadlock persisting after retrying {times} times.Stream: {streamId}, Expected version: {expectedVersion}";
    }
}