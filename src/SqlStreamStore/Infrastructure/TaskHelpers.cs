namespace StreamStore.Infrastructure
{
    using System.Threading.Tasks;

    public static class TaskHelpers
    {
        public static readonly Task CompletedTask = Task.FromResult(0);
    }
}