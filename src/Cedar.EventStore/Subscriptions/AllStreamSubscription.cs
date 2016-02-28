namespace Cedar.EventStore.Subscriptions
{
    using System;
    using System.Threading;
    using System.Threading.Tasks;
    using Cedar.EventStore.Infrastructure;

    public sealed class AllStreamSubscription : SubscriptionBase, IAllStreamSubscription
    {
        private long _nextCheckpoint;

        public AllStreamSubscription(
            long? fromCheckpoint,
            IReadOnlyEventStore readOnlyEventStore,
            IObservable<Unit> eventStoreAppendedNotification,
            StreamEventReceived streamEventReceived,
            SubscriptionDropped subscriptionDropped = null,
            string name = null)
            :base(readOnlyEventStore, eventStoreAppendedNotification, streamEventReceived, subscriptionDropped, name)
        {
            FromCheckpoint = fromCheckpoint;
            LastCheckpoint = fromCheckpoint;
            _nextCheckpoint = fromCheckpoint + 1 ?? Checkpoint.Start;
        }

        public long? FromCheckpoint { get; }

        public long? LastCheckpoint { get; private set; }

        public async Task Start(CancellationToken cancellationToken)
        {
            if(FromCheckpoint == Checkpoint.End)
            {
                // Get the last stream version and subscribe from there.
                var eventsPage = await ReadOnlyEventStore.ReadAllBackwards(
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
            Console.WriteLine($"Fetching from {_nextCheckpoint}");
            var allEventsPage = await ReadOnlyEventStore
                .ReadAllForwards(
                    _nextCheckpoint,
                    PageSize,
                    IsDisposed)
                .NotOnCapturedContext();
            bool isEnd = allEventsPage.IsEnd;

            Console.WriteLine($"Received {allEventsPage.StreamEvents.Length} events");
            foreach(var streamEvent in allEventsPage.StreamEvents)
            {
                if(IsDisposed.IsCancellationRequested)
                {
                    return true;
                }
                try
                {
                    await StreamEventReceived(streamEvent).NotOnCapturedContext();
                    LastCheckpoint = streamEvent.Checkpoint;
                    _nextCheckpoint = streamEvent.Checkpoint + 1;
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
            Console.WriteLine($"LastCheckpoint {LastCheckpoint}");
            return isEnd;
        }
    }
}
