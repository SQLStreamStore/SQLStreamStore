namespace Cedar.EventStore.Postgres
{
    internal static class InterlockedBooleanExtensions
    {
        internal static bool EnsureCalledOnce(this InterlockedBoolean interlockedBoolean)
        {
            return interlockedBoolean.CompareExchange(true, false);
        }
    }
}