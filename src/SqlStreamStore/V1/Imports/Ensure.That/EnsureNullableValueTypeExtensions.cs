#pragma warning disable CS1591 // Missing XML comment for publicly visible type or member

namespace SqlStreamStore.V1.Imports.Ensure.That
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