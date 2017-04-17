namespace SqlStreamStore.Imports.Ensure.That
{
    using System;
    using System.Diagnostics;

    public static partial class EnsureArg
    {
        [DebuggerStepThrough]
        public static void IsNotEmpty(Guid value, string paramName = Param.DefaultName)
        {
            if (!Ensure.IsActive)
                return;

            if (value.Equals(Guid.Empty))
                throw new ArgumentException(
                    ExceptionMessages.Guids_IsNotEmpty_Failed,
                    paramName);
        }
    }
}