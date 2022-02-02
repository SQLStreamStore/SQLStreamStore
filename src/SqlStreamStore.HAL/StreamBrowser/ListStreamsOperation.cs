namespace SqlStreamStore.HAL.StreamBrowser
{
    using System.Threading;
    using System.Threading.Tasks;
    using Microsoft.AspNetCore.Http;
    using SqlStreamStore.Streams;

    internal class ListStreamsOperation : IStreamStoreOperation<ListStreamsPage>
    {
        public Pattern Pattern { get; }
        public string ContinuationToken { get; }
        public int MaxCount { get; }
        public string PatternType { get; }
        public PathString Path { get; }

        public ListStreamsOperation(HttpContext context)
        {
            var request = context.Request;
            Path = request.Path;
            if(request.Query.TryGetValueCaseInsensitive('t', out var patternType))
            {
                PatternType = patternType;
            }

            request.Query.TryGetValueCaseInsensitive('p', out var pattern);

            switch(PatternType)
            {
                case "s":
                    Pattern = Pattern.StartsWith(pattern);
                    break;
                case "e":
                    Pattern = Pattern.EndsWith(pattern);
                    break;
                default:
                    Pattern = Pattern.Anything();
                    break;
            }

            if(request.Query.TryGetValueCaseInsensitive('c', out var continuationToken))
            {
                ContinuationToken = continuationToken;
            }

            MaxCount = request.Query.TryGetValueCaseInsensitive('m', out var m)
                       && int.TryParse(m, out var maxCount)
                ? maxCount
                : 100;
        }

        public Task<ListStreamsPage> Invoke(IStreamStore<ReadAllPage> streamStore, CancellationToken cancellationToken)
            => streamStore.ListStreams(Pattern, MaxCount, ContinuationToken, cancellationToken);
    }
}