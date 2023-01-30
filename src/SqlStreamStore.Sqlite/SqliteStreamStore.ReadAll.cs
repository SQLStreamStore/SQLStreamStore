namespace SqlStreamStore
{
    using System;
    using System.Linq;
    using System.Threading;
    using System.Threading.Tasks;
    using SqlStreamStore.Streams;

    public partial class SqliteStreamStore
    {
        protected override async Task<ReadAllPage> ReadAllForwardsInternal(
            long fromPositionExclusive,
            int maxCount,
            bool prefetch,
            CancellationToken cancellationToken,
            long fromMaxPositionInclusive = -1)
        {
            GuardAgainstDisposed();
            cancellationToken.ThrowIfCancellationRequested();

            using (var connection = OpenConnection())
            {
                // find starting node.
                var allStreamPosition = await connection.AllStream()
                    .HeadPosition(cancellationToken);
                if(allStreamPosition == Position.None)
                {
                    return new ReadAllPage(
                        Position.Start, 
                        Position.Start, 
                        true, 
                        ReadDirection.Forward);
                }

                if(allStreamPosition < fromPositionExclusive)
                {
                    return new ReadAllPage(
                        fromPositionExclusive, 
                        fromPositionExclusive, 
                        true, 
                        ReadDirection.Forward);
                }

                var remaining = await connection.AllStream()
                    .Remaining(ReadDirection.Forward, fromPositionExclusive);

                if(remaining == Position.End)
                {
                    return new ReadAllPage(
                        fromPositionExclusive,
                        Position.End,
                        true,
                        ReadDirection.Forward);
                }

                var messages = await connection.AllStream()
                    .Read(ReadDirection.Forward,
                        fromPositionExclusive,
                        maxCount, 
                        prefetch,
                        cancellationToken);
                

                bool isEnd = remaining - messages.Count <= 0;
                var nextPosition = messages.Any() 
                    ? messages.Last().Position + 1
                    : Position.End;

                return new ReadAllPage(
                    fromPositionExclusive,
                    nextPosition,
                    isEnd,
                    ReadDirection.Forward,
                    messages.ToArray());
            }
        }

        protected override async Task<ReadAllPage> ReadAllBackwardsInternal(
            long fromPosition,
            int maxCount,
            bool prefetch,
            CancellationToken cancellationToken)
        {
            GuardAgainstDisposed();
            cancellationToken.ThrowIfCancellationRequested();

            using (var connection = OpenConnection())
            {
                long? beginningPosition = fromPosition;
                var allStreamPosition = await connection.AllStream()
                    .HeadPosition(cancellationToken);
                if(allStreamPosition == Position.None)
                {
                    return new ReadAllPage(
                        Position.Start,
                        Position.Start,
                        true,
                        ReadDirection.Backward);
                }

                if(fromPosition == Position.End)
                {
                    beginningPosition = allStreamPosition > fromPosition ? allStreamPosition : fromPosition;
                }

                if(fromPosition > allStreamPosition && fromPosition > Position.Start)
                {
                    return new ReadAllPage(
                        fromPosition, 
                        fromPosition, 
                        true, 
                        ReadDirection.Backward);
                }
                
                // For reading $all, in the case where no events have been entered into
                // the root stream yet, we need to have a min beginning position of Position.Start (0).

                beginningPosition = beginningPosition < Position.Start
                    ? Position.Start
                    : beginningPosition;

                var remaining = await connection.AllStream()
                    .Remaining(ReadDirection.Backward, beginningPosition);
                if(remaining == Position.End)
                {
                    return new ReadAllPage(
                        allStreamPosition ?? Position.Start,
                        Position.End,
                        true,
                        ReadDirection.Backward);
                }

                var messages = await connection.AllStream()
                    .Read(ReadDirection.Backward,
                        beginningPosition,
                        maxCount,
                        prefetch,
                        cancellationToken);

                bool isEnd = remaining - messages.Count <= 0;
            
                var nextPosition = messages.Any() ? Math.Max(messages.Last().Position - 1, Position.Start) : Position.Start;

                return new ReadAllPage(
                    beginningPosition.Value,
                    nextPosition,
                    isEnd,
                    ReadDirection.Backward,
                    messages.ToArray());
            }
        }
    }
}