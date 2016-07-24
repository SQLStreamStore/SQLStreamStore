namespace SqlStreamStore.Subscriptions
{
    using System;
    using System.Threading;
    using System.Threading.Tasks;
    using SqlStreamStore.Infrastructure;
    using SqlStreamStore.Streams;
    using SqlStreamStore;

    public sealed class AllStreamSubscription : SubscriptionBase, IAllStreamSubscription
    {
        private long _nextCheckpoint;

        public AllStreamSubscription(
            long? fromCheckpoint,
            IReadonlyStreamStore readonlyStreamStore,
            IObservable<Unit> streamStoreAppendedNotification,
            StreamMessageReceived streamMessageReceived,
            SubscriptionDropped subscriptionDropped = null,
            string name = null)
            :base(readonlyStreamStore, streamStoreAppendedNotification, streamMessageReceived, subscriptionDropped, name)
        {
            FromCheckpoint = fromCheckpoint;
            LastCheckpoint = fromCheckpoint;
            _nextCheckpoint = fromCheckpoint + 1 ?? Checkpoint.Start;
        }

        public long? FromCheckpoint { get; }

        public long? LastCheckpoint { get; private set; }

        public override async Task Start(CancellationToken cancellationToken)
        {
            if(FromCheckpoint == Checkpoint.End)
            {
                // Get the last stream version and subscribe from there.
                var eventsPage = await ReadonlyStreamStore.ReadAllBackwards(
                    Checkpoint.End,
                    1,
                    cancellationToken).NotOnCapturedContext();

                // If fromCheckpoint = 0, we have empty store, so start from zero, otherwise, the next checkpoint is 
                // one after the FromCheckpoint.
                _nextCheckpoint = eventsPage.FromCheckpoint == 0 ?  0 : eventsPage.FromCheckpoint + 1;
            }
            await base.Start(cancellationToken).NotOnCapturedContext();
        }

        protected override async Task<bool> DoFetch()
        {
            var allMessagesPage = await ReadonlyStreamStore
                .ReadAllForwards(
                    _nextCheckpoint,
                    PageSize,
                    IsDisposed)
                .NotOnCapturedContext();
            bool isEnd = allMessagesPage.IsEnd;
            
            foreach(var streamMessage in allMessagesPage.Messages)
            {
                if(IsDisposed.IsCancellationRequested)
                {
                    return true;
                }
                try
                {
                    await StreamMessageReceived(streamMessage).NotOnCapturedContext();
                    LastCheckpoint = streamMessage.Checkpoint;
                    _nextCheckpoint = streamMessage.Checkpoint + 1;
                }
                catch(Exception ex)
                {
                    try
                    {
                        SubscriptionDropped.Invoke(ex.Message, ex);
                    }
                    catch
                    {
                        //TODO logging
                    }
                    finally
                    {
                        Dispose();
                    }
                }
            }
            
            return isEnd;
        }
    }
}
