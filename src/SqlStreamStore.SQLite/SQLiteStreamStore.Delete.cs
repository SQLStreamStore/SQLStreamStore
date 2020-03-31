namespace SqlStreamStore
{
    using System;
    using System.Data.SQLite;
    using System.Threading;
    using System.Threading.Tasks;

    public partial class SQLiteStreamStore
    {
        protected override async Task DeleteStreamInternal(
            string streamId, 
            int expectedVersion, 
            CancellationToken cancellationToken)
        {
            var streamIdInfo = new StreamIdInfo(streamId);

            using (var connection = await OpenConnection(cancellationToken))
            using (var transaction = connection.BeginTransaction())
            {


                transaction.Commit();
            }
        }

        protected override Task DeleteEventInternal(string streamId, Guid eventId, CancellationToken cancellationToken)
        {
            throw new NotImplementedException();
        }

        private async Task DeleteEventsInternal(
            StreamIdInfo streamIdInfo,
            Guid[] eventIds,
            SQLiteTransaction transaction,
            CancellationToken cancellationToken)
        {
            // TODO: Build deletion command.
        }
    }
}