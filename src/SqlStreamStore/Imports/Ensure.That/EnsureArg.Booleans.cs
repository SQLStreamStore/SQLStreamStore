namespace SqlStreamStore.Imports.Ensure.That
{
    using System;
    using System.Diagnostics;

    public static partial class EnsureArg
    {
        [DebuggerStepThrough]
        public static void IsTrue(bool value, string paramName = Param.DefaultName)
        {
            if (!Ensure.IsActive)
                return;

            if (!value)
                throw new ArgumentException(
                    ExceptionMessages.Booleans_IsTrueFailed,
                    paramName);
        }

        [DebuggerStepThrough]
        public static void IsFalse(bool value, string paramName = Param.DefaultName)
        {
            if (!Ensure.IsActive)
                return;

            if (value)
                throw new ArgumentException(
                    ExceptionMessages.Booleans_IsFalseFailed,
                    paramName);
        }
    }
}