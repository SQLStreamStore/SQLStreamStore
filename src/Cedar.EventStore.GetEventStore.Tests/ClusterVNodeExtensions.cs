namespace Cedar.EventStore
{
    using System.Threading.Tasks;
    using global::EventStore.Core;
    using global::EventStore.Core.Bus;
    using global::EventStore.Core.Messages;

    internal static class ClusterVNodeExtensions
    {
        internal static async Task StartAndWaitUntilInitialized(this ClusterVNode node)
        {
            var tcs = new TaskCompletionSource<int>();
            var handler = new AdHocHandler<UserManagementMessage.UserManagementServiceInitialized>(_ => tcs.TrySetResult(0));
            node.MainBus.Subscribe(handler);

            node.Start();

            await tcs.Task;

            node.MainBus.Unsubscribe(handler);
        } 
    }
}