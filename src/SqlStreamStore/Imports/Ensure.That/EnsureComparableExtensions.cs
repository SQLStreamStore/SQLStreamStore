namespace SqlStreamStore.Imports.Ensure.That
{
    using System;
    using System.Diagnostics;
    using SqlStreamStore.Imports.Ensure.That.Extensions;

    public static class EnsureComparableExtensions
    {
        [DebuggerStepThrough]
        public static Param<T> Is<T>(this Param<T> param, T expected) where T : struct, IComparable<T>
        {
            if (!Ensure.IsActive)
                return param;

            if (!param.Value.IsEq(expected))
                throw ExceptionFactory.CreateForParamValidation(param, ExceptionMessages.Comp_Is_Failed.Inject(param.Value, expected));

            return param;
        }

        [DebuggerStepThrough]
        public static Param<T> IsNot<T>(this Param<T> param, T expected) where T : struct, IComparable<T>
        {
            if (!Ensure.IsActive)
                return param;

            if (param.Value.IsEq(expected))
                throw ExceptionFactory.CreateForParamValidation(param, ExceptionMessages.Comp_IsNot_Failed.Inject(param.Value, expected));

            return param;
        }

        [DebuggerStepThrough]
        public static Param<T> IsLt<T>(this Param<T> param, T limit) where T : struct, IComparable<T>
        {
            if (!Ensure.IsActive)
                return param;

            if (!param.Value.IsLt(limit))
                throw ExceptionFactory.CreateForParamValidation(param, ExceptionMessages.Comp_IsNotLt.Inject(param.Value, limit));

            return param;
        }

        [DebuggerStepThrough]
        public static Param<T> IsLte<T>(this Param<T> param, T limit) where T : struct, IComparable<T>
        {
            if (!Ensure.IsActive)
                return param;

            if (param.Value.IsGt(limit))
                throw ExceptionFactory.CreateForParamValidation(param, ExceptionMessages.Comp_IsNotLte.Inject(param.Value, limit));

            return param;
        }

        [DebuggerStepThrough]
        public static Param<T> IsGt<T>(this Param<T> param, T limit) where T : struct, IComparable<T>
        {
            if (!Ensure.IsActive)
                return param;

            if (!param.Value.IsGt(limit))
                throw ExceptionFactory.CreateForParamValidation(param, ExceptionMessages.Comp_IsNotGt.Inject(param.Value, limit));

            return param;
        }

        [DebuggerStepThrough]
        public static Param<T> IsGte<T>(this Param<T> param, T limit) where T : struct, IComparable<T>
        {
            if (!Ensure.IsActive)
                return param;

            if (param.Value.IsLt(limit))
                throw ExceptionFactory.CreateForParamValidation(param, ExceptionMessages.Comp_IsNotGte.Inject(param.Value, limit));

            return param;
        }

        [DebuggerStepThrough]
        public static Param<T> IsInRange<T>(this Param<T> param, T min, T max) where T : struct, IComparable<T>
        {
            if (!Ensure.IsActive)
                return param;

            if (param.Value.IsLt(min))
                throw ExceptionFactory.CreateForParamValidation(param, ExceptionMessages.Comp_IsNotInRange_ToLow.Inject(param.Value, min));

            if (param.Value.IsGt(max))
                throw ExceptionFactory.CreateForParamValidation(param, ExceptionMessages.Comp_IsNotInRange_ToHigh.Inject(param.Value, max));

            return param;
        }
    }
}