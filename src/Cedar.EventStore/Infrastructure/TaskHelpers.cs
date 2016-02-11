namespace Cedar.EventStore.Infrastructure
{
    using System.Threading.Tasks;

    public static class TaskHelpers
    {
        public static Task CompletedTask = Task.FromResult(0);
    }
}