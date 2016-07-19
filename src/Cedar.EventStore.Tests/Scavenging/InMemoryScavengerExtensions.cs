namespace Cedar.EventStore.Scavenging
{
    using System;
    using System.Threading.Tasks;
    using Cedar.EventStore.Streams;

    internal static class InMemoryScavengerExtensions
    {
        internal static Task<StreamEvent> WaitForStreamEventProcessed(
            this InMemoryScavenger scavenger,
            Predicate<StreamEvent> predicate = null)
        {
            predicate = predicate ?? (_ => true);
            var tcs = new TaskCompletionSource<StreamEvent>();
            EventHandler<StreamEvent> handler = (sender, streamEvent) =>
            {
                if(predicate(streamEvent))
                {
                    tcs.SetResult(streamEvent);
                }
            };
            scavenger.StreamEventProcessed += handler;
            return tcs.Task;
        }
    }
}