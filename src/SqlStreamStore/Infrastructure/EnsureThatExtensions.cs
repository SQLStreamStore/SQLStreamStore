namespace SqlStreamStore.Infrastructure
{
    using SqlStreamStore.Imports.Ensure.That;

    internal static class EnsureThatExtensions
    {
        internal static Param<string> DoesNotStartWith(this Param<string> param, string s)
        {
            if (!Ensure.IsActive)
            {
                return param;
            }
            if (param.Value.StartsWith(s))
            {
                throw ExceptionFactory.CreateForParamValidation(param, $"Must not start with{s}");
            }
            return param;
        }
    }
}