namespace SqlStreamStore.Infrastructure
{
    using EnsureThat;

    internal static class EnsureThatExtensions
    {
        internal static Param<string> DoesNotStartWith(this Param<string> param, string s)
        {
            if(param.Value.StartsWith(s))
            {
                throw ExceptionFactory.CreateForParamValidation(param, $"Must not start with{s}");
            }
            return param;
        }
    }
}