#pragma warning disable CS1591 // Missing XML comment for publicly visible type or member

namespace SqlStreamStore.Imports.Ensure.That
{
    using System;
    using System.Diagnostics;

    public static class EnsureGuidExtensions
    {
        [DebuggerStepThrough]
        public static Param<Guid> IsNotEmpty(this Param<Guid> param)
        {
            if (!Ensure.IsActive)
                return param;

            if (param.Value.Equals(Guid.Empty))
                throw ExceptionFactory.CreateForParamValidation(param, ExceptionMessages.Guids_IsNotEmpty_Failed);

            return param;
        }
    }
}