namespace SqlStreamStore.Infrastructure
{
    using SqlStreamStore.Imports.Ensure.That;

    internal static class EnsureStringExtensions
    {
        internal static Param<string> DoesNotContainWhitespace(this Param<string> param)
        {
            if(!Ensure.IsActive)
            {
                return param;
            }
            if(param.Value != null && param.Value.Contains(" "))
            {
                throw ExceptionFactory.CreateForParamNullValidation(param, "Value cannot contain whitespace");
            }
            return param;
        }
    }
}