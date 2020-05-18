namespace SqlStreamStore
{
    using System.Linq;
    using System.Threading;
    using System.Threading.Tasks;
    using SqlStreamStore.Streams;

    public partial class SqliteStreamStore
    {
        protected override async Task<ListStreamsPage> ListStreamsInternal(
            Pattern pattern,
            int maxCount,
            string continuationToken,
            ListNextStreamsPage listNextStreamsPage,
            CancellationToken cancellationToken)
        {
            GuardAgainstDisposed();
            cancellationToken.ThrowIfCancellationRequested();

            using(var connection = OpenConnection())
            {
                var headers = await connection.AllStream()
                    .List(pattern, maxCount, continuationToken, cancellationToken);

                var token = headers.Any()
                    ? headers.Last().Key.ToString()
                    : Position.End.ToString();

                return new ListStreamsPage(token, 
                    headers.Select(h => h.IdOriginal).ToArray(), 
                    listNextStreamsPage);
            }
        }
    }
}