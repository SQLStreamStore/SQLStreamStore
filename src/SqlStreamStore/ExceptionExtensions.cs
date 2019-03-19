namespace SqlStreamStore
{
    using System;

    internal static class ExceptionExtensions
    {
        public static bool IsOperationCancelledException(this Exception ex)
            => ex is OperationCanceledException || (ex.InnerException?.IsOperationCancelledException() ?? false);
    }
}