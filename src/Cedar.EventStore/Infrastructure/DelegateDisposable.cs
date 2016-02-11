namespace Cedar.EventStore.Infrastructure
{
    using System;

    public class DelegateDisposable : IDisposable
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