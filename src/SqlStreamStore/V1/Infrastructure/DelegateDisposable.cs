namespace SqlStreamStore.V1.Infrastructure
{
    using System;

    internal class DelegateDisposable : IDisposable
    {
        private readonly Action _action;

        public DelegateDisposable(Action action)
        {
            _action = action;
        }

        public void Dispose()
        {
            _action();
        }
    }
}