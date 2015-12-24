namespace Cedar.EventStore
{
    using System.Threading.Tasks;
    using global::EventStore.Core;
    using global::EventStore.Core.Bus;
    using global::EventStore.Core.Messages;

    internal static class ClusterVNodeExtensions
    {
        internal static Task<ClusterVNode> StartAndWaitUntilInitialized(this ClusterVNode node)
        {
            var tcs = new TaskCompletionSource<ClusterVNode>();

            node.MainBus
                .Subscribe(new AdHocHandler<UserManagementMessage.UserManagementServiceInitialized>(
                    _ => tcs.TrySetResult(node)));

            node.Start();

            return tcs.Task;
        } 
    }
}