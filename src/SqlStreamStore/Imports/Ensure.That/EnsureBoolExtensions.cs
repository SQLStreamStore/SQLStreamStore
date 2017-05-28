#pragma warning disable CS1591 // Missing XML comment for publicly visible type or member

namespace SqlStreamStore.Imports.Ensure.That
{
    using System.Diagnostics;

    public static class EnsureBoolExtensions
    {
        [DebuggerStepThrough]
        public static Param<bool> IsTrue(this Param<bool> param)
        {
            if (!Ensure.IsActive)
                return param;

            if (!param.Value)
                throw ExceptionFactory.CreateForParamValidation(param, ExceptionMessages.Booleans_IsTrueFailed);

            return param;
        }

        [DebuggerStepThrough]
        public static Param<bool> IsFalse(this Param<bool> param)
        {
            if (!Ensure.IsActive)
                return param;

            if (param.Value)
                throw ExceptionFactory.CreateForParamValidation(param, ExceptionMessages.Booleans_IsFalseFailed);

            return param;
        }
    }
}