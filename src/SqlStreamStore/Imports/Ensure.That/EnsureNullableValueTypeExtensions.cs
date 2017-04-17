namespace SqlStreamStore.Imports.Ensure.That
{
    using System.Diagnostics;

    public static class EnsureNullableValueTypeExtensions
    {
        [DebuggerStepThrough]
        public static Param<T?> IsNotNull<T>(this Param<T?> param) where T : struct
        {
            if (!Ensure.IsActive)
                return param;

            if (param.Value == null)
                throw ExceptionFactory.CreateForParamNullValidation(param, ExceptionMessages.Common_IsNotNull_Failed);

            return param;
        }
    }
}