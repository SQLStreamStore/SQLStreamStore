namespace SqlStreamStore.V1
{
    using System.Threading.Tasks;

    internal static class SkippedPayload
    {
        public static readonly Task<string> Instance = Task.FromResult<string>(null);
    }
}