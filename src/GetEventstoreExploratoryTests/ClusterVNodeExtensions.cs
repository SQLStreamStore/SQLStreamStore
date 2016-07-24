namespace GetEventstoreExploratoryTests
{
    using System.Threading.Tasks;
    using EventStore.Core;
    using EventStore.Core.Bus;
    using EventStore.Core.Messages;

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