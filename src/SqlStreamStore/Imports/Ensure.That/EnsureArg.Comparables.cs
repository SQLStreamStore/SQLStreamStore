namespace SqlStreamStore.Imports.Ensure.That
{
    using System;
    using System.Diagnostics;
    using SqlStreamStore.Imports.Ensure.That.Extensions;

    public static partial class EnsureArg
    {
        [DebuggerStepThrough]
        public static void Is<T>(T param, T expected, string paramName = Param.DefaultName) where T : struct, IComparable<T>
        {
            if (!Ensure.IsActive)
                return;

            if (!param.IsEq(expected))
                throw new ArgumentException(ExceptionMessages.Comp_Is_Failed.Inject(param, expected), paramName);
        }

        [DebuggerStepThrough]
        public static void IsNot<T>(T param, T expected, string paramName = Param.DefaultName) where T : struct, IComparable<T>
        {
            if (!Ensure.IsActive)
                return;

            if (param.IsEq(expected))
                throw new ArgumentException(ExceptionMessages.Comp_IsNot_Failed.Inject(param, expected), paramName);
        }

        [DebuggerStepThrough]
        public static void IsLt<T>(T param, T limit, string paramName = Param.DefaultName) where T : struct, IComparable<T>
        {
            if (!Ensure.IsActive)
                return;

            if (!param.IsLt(limit))
                throw new ArgumentException(ExceptionMessages.Comp_IsNotLt.Inject(param, limit), paramName);
        }

        [DebuggerStepThrough]
        public static void IsLte<T>(T param, T limit, string paramName = Param.DefaultName) where T : struct, IComparable<T>
        {
            if (!Ensure.IsActive)
                return;

            if (param.IsGt(limit))
                throw new ArgumentException(ExceptionMessages.Comp_IsNotLte.Inject(param, limit), paramName);
        }

        [DebuggerStepThrough]
        public static void IsGt<T>(T param, T limit, string paramName = Param.DefaultName) where T : struct, IComparable<T>
        {
            if (!Ensure.IsActive)
                return;

            if (!param.IsGt(limit))
                throw new ArgumentException(ExceptionMessages.Comp_IsNotGt.Inject(param, limit), paramName);
        }

        [DebuggerStepThrough]
        public static void IsGte<T>(T param, T limit, string paramName = Param.DefaultName) where T : struct, IComparable<T>
        {
            if (!Ensure.IsActive)
                return;

            if (param.IsLt(limit))
                throw new ArgumentException(ExceptionMessages.Comp_IsNotGte.Inject(param, limit), paramName);
        }

        [DebuggerStepThrough]
        public static void IsInRange<T>(T param, T min, T max, string paramName = Param.DefaultName) where T : struct, IComparable<T>
        {
            if (!Ensure.IsActive)
                return;

            if (param.IsLt(min))
                throw new ArgumentException(ExceptionMessages.Comp_IsNotInRange_ToLow.Inject(param, min), paramName);

            if (param.IsGt(max))
                throw new ArgumentException(ExceptionMessages.Comp_IsNotInRange_ToHigh.Inject(param, max), paramName);
        }
    }
}
