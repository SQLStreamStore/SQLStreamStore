namespace SqlStreamStore.Imports.Ensure.That
{
    using System.Diagnostics;

    public static class EnsureValueTypeExtensions
    {
        [DebuggerStepThrough]
        public static Param<T> IsNotDefault<T>(this Param<T> param) where T : struct
        {
            if (!Ensure.IsActive)
                return param;

            if (default(T).Equals(param.Value))
                throw ExceptionFactory.CreateForParamValidation(param, ExceptionMessages.ValueTypes_IsNotDefault_Failed);

            return param;
        }
    }
}